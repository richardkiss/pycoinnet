import hashlib

from pycoin.encoding.hash import hash160
from pycoin.networks.registry import network_for_netcode


class MultisigKey:
    STANDARD = "std"
    SEGWIT_P2S_ENCAPSULATED = "swe"
    SEGWIT_NATIVE = "swn"

    def __init__(self, network, bip32_list, collection_type=STANDARD, sig_count_required=1, path=""):
        self._network = network
        self._bip32_list = bip32_list
        self._collection_type = collection_type
        self._m = sig_count_required
        self._path = path

    def address_preimages_interests(self):
        subkeys = [_.subkey_for_path(self._path) for _ in self._bip32_list]
        if len(subkeys) == 1:
            key = subkeys[0]
            h160 = key.hash160()
            if self._collection_type == self.STANDARD:
                return self._network.ui.address_for_p2pkh(h160), [], subkeys
            if self._collection_type == self.SEGWIT_P2S_ENCAPSULATED:
                script = self._network.ui._script_info.script_for_p2pkh_wit(h160)
                return self._network.ui.address_for_p2s(script), [script], subkeys
            if self._collection_type == self.SEGWIT_NATIVE:
                return self._network.ui.address_for_p2pkh_wit(h160), [], subkeys
        else:
            sec_keys = sorted([_.sec() for _ in subkeys])
            multisig_script = self._network.ui._script_info.script_for_multisig(self._m, sec_keys)
            h160 = hash160(multisig_script)
            if self._collection_type == self.STANDARD:
                return self._network.ui.address_for_p2s(multisig_script), [multisig_script], subkeys
            if self._collection_type == self.SEGWIT_P2S_ENCAPSULATED:
                witness_script = self._network.ui._script_info.script_for_p2s_wit(multisig_script)
                return self._network.ui.address_for_p2s(witness_script), [witness_script, multisig_script], subkeys
            if self._collection_type == self.SEGWIT_NATIVE:
                h256 = hashlib.sha256(multisig_script).digest()
                return self._network.ui.address_for_p2s_wit(multisig_script), [multisig_script], subkeys
        return None, []

    def subkey_for_path(self, path):
        new_path = (self._path + path).replace("//", "/")
        return self.__class__(self._network, self._bip32_list, self._collection_type, self._m, new_path)

    def as_text(self):
        items = [self._network.code, self._collection_type, self._m,
                 "/".join(_.hwif(as_private=False) for _ in self._bip32_list)]
        suffix = ""
        if self._path:
            suffix = ":" + self._path
        return ":".join("%s" % _ for _ in items) + suffix


def parse_MultisigKey(text):
    try:
        items = text.split(":")
        network = network_for_netcode(items[0])
        m = int(items[2])
        bip32_list = [network.ui.parse(_, types=["bip32"]) for _ in items[3].split("/")]
        path = ""
        if len(items) > 4:
            path = items[5]
        return MultisigKey(network, bip32_list, collection_type=items[1], sig_count_required=m, path=path)

    except Exception:
        pass
