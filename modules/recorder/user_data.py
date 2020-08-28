from typing import Optional


class UserData:
    def __init__(self, **kwargs):
        self.name: str = kwargs.pop('name')
        self._display_name: str = kwargs.pop('display_name', '')
        self.id: str = kwargs.pop('id', '')

    @property
    def display_name(self):
        if self._display_name:
            return self._display_name
        return self.name

    @classmethod
    def from_json(cls, data: Optional[dict]):
        if not data:
            return None
        return cls(
            display_name=data.get('display_name'),
            id=data.get('_id'),
            name=data.get('name'),
        )
