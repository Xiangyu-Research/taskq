"""Test training variants (the toy recipe's variant): variants reading SPLITS_DIR need sample subsets from the repo, so the relay plugin does not send them (relay_plugins.repo_files)."""


class V_save:
    pass


class V_save_n470(V_save):
    SPLITS_DIR = "subsets"


class V_m(V_save):
    pass


class V_s(V_save):
    pass


class V_st(V_save):
    pass
