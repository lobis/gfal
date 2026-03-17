import stat


def file_type_str(ftype):
    return {
        stat.S_IFBLK: "block device",
        stat.S_IFCHR: "character device",
        stat.S_IFDIR: "directory",
        stat.S_IFIFO: "fifo",
        stat.S_IFLNK: "symbolic link",
        stat.S_IFREG: "regular file",
        stat.S_IFSOCK: "socket",
    }.get(ftype, "unknown")


def _mode_triplet(mode):
    s = ["-", "-", "-"]
    if mode & stat.S_IROTH:
        s[0] = "r"
    if mode & stat.S_IWOTH:
        s[1] = "w"
    if mode & stat.S_IXOTH:
        s[2] = "x"
    return "".join(s)


def file_mode_str(mode):
    if stat.S_ISDIR(mode):
        prefix = "d"
    elif stat.S_ISBLK(mode):
        prefix = "b"
    elif stat.S_ISCHR(mode):
        prefix = "c"
    elif stat.S_ISFIFO(mode):
        prefix = "f"
    elif stat.S_ISSOCK(mode):
        prefix = "s"
    else:
        prefix = "-"
    return (
        prefix
        + _mode_triplet(mode >> 6)
        + _mode_triplet(mode >> 3)
        + _mode_triplet(mode)
    )
