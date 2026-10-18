"""A credential in a collected command is masked on every read path, with no privacy rules configured."""
import atuin


def exercise(h):
    atuin.exercise(h, credential_output=True)
