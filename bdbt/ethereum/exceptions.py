class ABITypeNotValid(Exception):
    """
    We failed to create ABI type
    """

    def __init__(self, message) -> None:
        super().__init__(message)


class TargetItemNotFound(Exception):
    """
    We found no target item in ABI
    """

    def __init__(self, message) -> None:
        super().__init__(message)
