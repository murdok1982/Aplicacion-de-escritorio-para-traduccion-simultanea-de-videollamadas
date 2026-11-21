"""Mezclador PCM a 48 kHz con ducking determinista, sin hardware (ADR-002).

Contrato (SPEC.md §REQ-05 / AC-08, ARCHITECTURE.md §2.1, §7 ADR-002):

* El monitor de ``Translator_Rx_Sink`` (señal original a 48 kHz) se atenúa a
  ``duck_level`` mientras suena el doblaje y vuelve a 1.0 al terminar.
* El TTS (ya resampleado a 48 kHz por :mod:`client.audio.resampler`) se mezcla
  a ganancia 1.0. Resultado: ``original * gain + tts``.
* ``gain`` sigue una rampa lineal de ``ramp_ms`` (30 ms por defecto = 1440
  muestras a 48 kHz) para eliminar los clicks de ataque/liberación.
* Si el pico de la mezcla supera 1.0 se aplica un **escaleo de pico global**
  (``mix / peak``): no hay recorte duro (hard clipping) y la forma de onda se
  conserva. Nunca se emiten muestras fuera de ``[-1.0, 1.0]``.
* Los flags ``Flags.DUCK_START`` / ``Flags.DUCK_STOP`` de
  :mod:`shared.protocol` gobiernan el estado persistente del mezclador.

**Testabilidad sin hardware (ADR-002):** esta clase es pura en el sentido de
que no abre dispositivos, no ejecuta ``pactl``/``pw-cli`` y no importa
PipeWire. Toda su entrada es PCM numpy o ``bytes`` y toda su salida es
determinista; por eso los tests de integración usan únicamente fixtures WAV
(``tone_1k_48k.wav``, ``voice_es_16k.wav``, ``echo_48k.wav``,
``silence_16k.wav``) y ``client.audio.resampler`` para llevar el TTS a 48 kHz.
La única dependencia numpy externa es la ya fijada en ``pyproject.toml``.
"""

from __future__ import annotations

from typing import Any, cast

import numpy as np
from numpy.typing import NDArray

from client.audio.resampler import resample_to_48k
from shared.constants import PIPEWIRE_SAMPLE_RATE
from shared.protocol import Flags

DUCK_LEVEL_MIN: float = 0.10
DUCK_LEVEL_MAX: float = 0.15
DEFAULT_DUCK_LEVEL: float = 0.12
DEFAULT_RAMP_MS: float = 30.0

_GAIN_UNDUCKED: float = 1.0
_PEAK_LIMIT: float = 1.0
_FLAGS_MAX: int = 0xFF

# PCM 48 kHz: float normalizado, int16 en LSB o bytes int16 LE (2-D = mono
# downmix de 1-2 canales, igual que en el resampler).
type AudioSamples = (
    bytes
    | bytearray
    | memoryview
    | NDArray[np.float32]
    | NDArray[np.float64]
    | NDArray[np.int16]
)
# La salida sigue el tipo del argumento ``original``: bytes int16 LE para el
# transporte PipeWire, float32 normalizado para procesamiento en memoria.
type MixOutput = bytes | NDArray[np.float32]


def _validate_duck_level(value: float) -> float:
    """Valida el nivel de ducking en la frontera (rango legal 0.10-0.15)."""
    if isinstance(value, bool) or not isinstance(value, int | float | np.floating):
        raise ValueError(
            f"duck_level debe ser un número real, recibido {type(value).__name__}"
        )
    level = float(value)
    if not np.isfinite(level) or not DUCK_LEVEL_MIN <= level <= DUCK_LEVEL_MAX:
        raise ValueError(
            f"duck_level fuera de rango [{DUCK_LEVEL_MIN}, {DUCK_LEVEL_MAX}]: {value!r}"
        )
    return level


def _validate_ramp_ms(value: float) -> float:
    """Valida la duración de la rampa en milisegundos."""
    if isinstance(value, bool) or not isinstance(value, int | float | np.floating):
        raise ValueError(f"ramp_ms debe ser un número real, recibido {type(value).__name__}")
    ramp = float(value)
    if not np.isfinite(ramp) or ramp <= 0.0:
        raise ValueError(f"ramp_ms debe ser finito y > 0: {value!r}")
    return ramp


class DuckingMixer:
    """Mezcla ``original * gain + tts`` con ducking y rampa sin clicks.

    Uso típico en el daemon (sin hardware involucrado)::

        mixer = DuckingMixer()                      # duck_level=0.12
        mixer.apply_flags(int(Flags.DUCK_START))    # o mixer.on_duck_start()
        mix = mixer.process(original_48k, tts_48k, ducking_active=True)
        mixer.on_duck_stop()
        mix = mixer.process(original_48k, tts_48k, ducking_active=False)

    El argumento ``ducking_active`` de :meth:`process` es la autoridad por
    llamada y sincroniza los flags; :meth:`on_duck_start` y
    :meth:`on_duck_stop` permiten además manejar el evento ``duck`` del
    WebSocket sin depender del argumento. El estado es íntegramente observable
    mediante :attr:`gain`, :attr:`target_gain`, :attr:`flags` y
    :attr:`ducking_active`, lo que hace verificables AC-08 y §7 sin PipeWire.
    """

    def __init__(
        self,
        duck_level: float = DEFAULT_DUCK_LEVEL,
        *,
        ramp_ms: float = DEFAULT_RAMP_MS,
        sample_rate: int = PIPEWIRE_SAMPLE_RATE,
    ) -> None:
        if isinstance(sample_rate, bool) or sample_rate != PIPEWIRE_SAMPLE_RATE:
            raise ValueError(
                f"sample_rate debe ser {PIPEWIRE_SAMPLE_RATE} Hz (ADR-005), "
                f"recibido {sample_rate!r}"
            )
        self._duck_level = _validate_duck_level(duck_level)
        self._ramp_ms = _validate_ramp_ms(ramp_ms)
        self._ramp_samples = max(1, int(round(PIPEWIRE_SAMPLE_RATE * self._ramp_ms / 1000.0)))
        self._gain: float = _GAIN_UNDUCKED
        self._target_gain: float = _GAIN_UNDUCKED
        self._step: float = 0.0
        self._flags: int = 0

    @property
    def duck_level(self) -> float:
        """Nivel de atenuación del original mientras dura el doblaje."""
        return self._duck_level

    @property
    def ramp_ms(self) -> float:
        """Duración de la rampa de ataque/liberación en milisegundos."""
        return self._ramp_ms

    @property
    def ramp_samples(self) -> int:
        """Muestras de 48 kHz que dura la rampa (30 ms → 1440)."""
        return self._ramp_samples

    @property
    def sample_rate(self) -> int:
        """Tasa canónica de entrada/salida del mezclador."""
        return PIPEWIRE_SAMPLE_RATE

    @property
    def gain(self) -> float:
        """Ganancia instantánea aplicada al original (1.0 si no hay ducking)."""
        return self._gain

    @property
    def target_gain(self) -> float:
        """Ganancia hacia la que converge la rampa."""
        return self._target_gain

    @property
    def flags(self) -> int:
        """Máscara con los flags de ducking del frame de protocolo."""
        return self._flags

    @property
    def ducking_active(self) -> bool:
        """``True`` mientras el flag ``DUCK_START`` esté activo."""
        return bool(self._flags & int(Flags.DUCK_START))

    def _set_target(self, target: float) -> None:
        """Recalcula el paso por muestra solo cuando cambia el objetivo."""
        if target == self._target_gain:
            return
        self._target_gain = target
        self._step = (target - self._gain) / self._ramp_samples

    def on_duck_start(self) -> None:
        """Activa ``DUCK_START``, limpia ``DUCK_STOP`` y baja la rampa."""
        self._flags = int(Flags.DUCK_START)
        self._set_target(self._duck_level)

    def on_duck_stop(self) -> None:
        """Activa ``DUCK_STOP``, limpia ``DUCK_START`` y sube la rampa."""
        self._flags = int(Flags.DUCK_STOP)
        self._set_target(_GAIN_UNDUCKED)

    def apply_flags(self, flags: int) -> None:
        """Aplica los bits de ducking de una cabecera ``!2sBBHIIH``.

        Se ignoran bits ajenos al ducking (``FINAL``, ``PTT_ACTIVE``). Si
        llegan ``DUCK_START`` y ``DUCK_STOP`` a la vez prevalece ``DUCK_STOP``
        por seguridad: se restaura el volumen completo del original.
        """
        if isinstance(flags, bool) or not isinstance(flags, int | np.integer):
            raise ValueError(
                f"flags debe ser un entero uint8, recibido {type(flags).__name__}"
            )
        value = int(flags)
        if not 0 <= value <= _FLAGS_MAX:
            raise ValueError(f"flags fuera de uint8: {value!r}")
        if value & int(Flags.DUCK_START):
            self.on_duck_start()
        if value & int(Flags.DUCK_STOP):
            self.on_duck_stop()

    def reset(self) -> None:
        """Vuelve al estado inicial: ganancia 1.0, sin flags, rampa detenida."""
        self._gain = _GAIN_UNDUCKED
        self._target_gain = _GAIN_UNDUCKED
        self._step = 0.0
        self._flags = 0

    @staticmethod
    def _as_float32(samples: AudioSamples, *, name: str) -> NDArray[np.float32]:
        """Normaliza PCM 48 kHz a float32 mono reutilizando el resampler."""
        normalized = resample_to_48k(
            samples, src_rate=PIPEWIRE_SAMPLE_RATE, dtype=np.float32
        )
        return cast("NDArray[np.float32]", normalized)

    def _ramp_gains(self, count: int) -> NDArray[np.float64]:
        """Genera ``count`` ganancias consecutivas y avanza el estado interno."""
        steps = np.arange(1, count + 1, dtype=np.float64)
        candidate = self._gain + self._step * steps
        low = min(self._gain, self._target_gain)
        high = max(self._gain, self._target_gain)
        gains = np.clip(candidate, low, high)
        self._gain = float(gains[-1])
        if self._gain == self._target_gain:
            self._step = 0.0
        return gains

    def process(
        self,
        original: AudioSamples,
        tts: AudioSamples,
        *,
        ducking_active: bool,
    ) -> MixOutput:
        """Mezcla dos bloques PCM a 48 kHz con la rampa de ducking vigente.

        ``ducking_active`` fija el objetivo de la rampa (``duck_level`` o 1.0)
        y actualiza los flags del protocolo. Las señales cortas se rellenan
        con ceros hasta la longitud de la más larga; el resultado sigue el
        tipo de ``original`` (``bytes`` int16 LE o ``NDArray[np.float32]``).
        Toda entrada se valida en :func:`client.audio.resampler.resample_to_48k`
        (dtype, forma, finitud y paridad de bytes).
        """
        if not isinstance(ducking_active, bool):
            raise ValueError(
                f"ducking_active debe ser bool, recibido {type(ducking_active).__name__}"
            )

        original_f = self._as_float32(original, name="original")
        tts_f = self._as_float32(tts, name="tts")

        if ducking_active:
            self.on_duck_start()
        else:
            self.on_duck_stop()

        count = max(original_f.size, tts_f.size)
        if count == 0:
            return b"" if isinstance(original, bytes | bytearray | memoryview) else (
                cast("NDArray[np.float32]", np.empty(0, dtype=np.float32))
            )

        if original_f.size < count:
            original_f = cast(
                "NDArray[np.float32]",
                np.pad(original_f, (0, count - original_f.size)),
            )
        if tts_f.size < count:
            tts_f = cast(
                "NDArray[np.float32]",
                np.pad(tts_f, (0, count - tts_f.size)),
            )

        gains = self._ramp_gains(count)
        mix: NDArray[Any] = original_f.astype(np.float64) * gains + tts_f.astype(np.float64)

        peak = float(np.max(np.abs(mix)))
        if peak > _PEAK_LIMIT:
            # Escaleo global: evita hard clipping y preserva la forma de onda.
            mix = mix / peak

        mixed = cast("NDArray[np.float32]", np.ascontiguousarray(mix, dtype=np.float32))
        if isinstance(original, bytes | bytearray | memoryview):
            pcm16 = cast(
                "NDArray[np.int16]",
                resample_to_48k(mixed, src_rate=PIPEWIRE_SAMPLE_RATE, dtype=np.int16),
            )
            return pcm16.tobytes()
        return mixed
