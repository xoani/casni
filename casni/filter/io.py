from dataclasses import dataclass
from .primitive import Filter, Modifier

@dataclass
class InputFilter:
    name: str
    dataclass: str #replace to ENUM
    filter: Filter
    level: str = 'file' #replace to ENUM (SESSION, FILE) 
    shared: str = 'subject' #replaced to ENUM (SUBJECT, SESSION, MODAL, REGEX)
    main_input: bool = False

@dataclass
class OutputModifier:
    name: str
    modifier: Modifier