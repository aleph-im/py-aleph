import os.path

from p2pclient.libp2p_stubs.crypto.rsa import (
    KeyPair,
    create_new_key_pair,
)


def generate_keypair() -> KeyPair:
    """
    Generates a new key pair for the node.
    """
    key_pair = create_new_key_pair()
    return key_pair


def save_keys(key_pair: KeyPair, key_dir: str) -> None:
    """
    Saves the private and public keys to the specified directory. The keys are stored in 3 formats:
    - The private key is stored in PEM format for ease of use, and in a serialized format compatible with the P2P
    daemon (DER + protobuf encoding).
    - The public key is stored in PEM format.
    """
    # Create the key directory if it does not exist
    if os.path.exists(key_dir):
        if not os.path.isdir(key_dir):
            raise NotADirectoryError(f"Key directory ({key_dir}) is not a directory")
    else:
        os.makedirs(key_dir)

    # Save the private and public keys in the key directory, as well as the serialized private key for p2pd.
    private_key_path = os.path.join(key_dir, "node-secret.key")
    public_key_path = os.path.join(key_dir, "node-pub.key")
    serialized_key_path = os.path.join(key_dir, "serialized-node-secret.key")

    with open(private_key_path, "wb") as key_file:
        key_file.write(key_pair.private_key.impl.export_key())

    with open(public_key_path, "wb") as key_file:
        key_file.write(key_pair.public_key.impl.export_key())

    with open(serialized_key_path, "wb") as f:
        f.write(key_pair.private_key.serialize())
