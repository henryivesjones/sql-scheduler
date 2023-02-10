def w_print(content: str, end="\n"):
    print("\x1b[2K\r", end="")
    print(content, end=end)
