import webrtcvad
import struct


RMS_THRESHOLD = 100  # tune this if too aggressive or too lenient


def get_rms(pcm_chunk: bytes) -> float:
    """
    Calculate RMS energy of a PCM16 LE chunk (16-bit signed samples).
    Replaces audioop.rms(pcm_chunk, 2) with pure Python.
    """
    try:
        n = len(pcm_chunk) // 2          # number of 16-bit samples
        if n == 0:
            return 0.0
        samples = struct.unpack(f"<{n}h", pcm_chunk[: n * 2])
        rms = (sum(s * s for s in samples) / n) ** 0.5
        return rms
    except Exception:
        return 0.0

vad = webrtcvad.Vad(2)
def is_speech(pcm_chunk: bytes) -> bool:
    """
    Two-stage voice detection:
    1. RMS energy gate — filters distant noise, background TV, breathing
       A frame must have minimum energy to even be considered speech.
    2. webrtcvad — confirms it's actually voice, not just loud noise

    Both must pass for the frame to be counted as speech.
    """
    try:
        # Stage 1: energy gate
        # Rejects ambient noise, distant sounds, mic handling
        rms = get_rms(pcm_chunk[:320] if len(pcm_chunk) >= 320 else pcm_chunk)
        if rms < RMS_THRESHOLD:
            return False

        # Stage 2: webrtcvad at max aggressiveness
        if len(pcm_chunk) < 320:
            pcm_chunk = pcm_chunk + b'\x00' * (320 - len(pcm_chunk))
        return vad.is_speech(pcm_chunk[:320], sample_rate=8000)

    except Exception:
        return False

