from dataclasses import dataclass


@dataclass
class Contract:
    abi: str
    address: str
    name: str
