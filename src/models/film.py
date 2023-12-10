# Используем pydantic для упрощения работы при перегонке данных из json в объекты
from pydantic import BaseModel


class Film(BaseModel):
    id: str
    title: str
    description: str
