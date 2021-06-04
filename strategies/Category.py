import enum


class Category(enum.Enum):
    GROWTH_AND_QUALITY = {"name": "Growth and Quality", "description": ""}
    CUSTOM = {"name": "Custom", "description": ""}
    VALUE = {"name": "Value", "description": ""}
    GENERAL = {"name": "General", "description": ""}
    NONE = {"name": "", "description": ""}
