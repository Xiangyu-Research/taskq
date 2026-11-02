"""测试用的训练变体（toy 做法的 variant）：读 SPLITS_DIR 的变体要仓库里的样本子集，中继插件不送（relay_plugins.repo_files）。"""


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
