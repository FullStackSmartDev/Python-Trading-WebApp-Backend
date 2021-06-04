import csv
import io
import yaml
import os

from .Category import Category
from .Type import Type


class Strategy:
    @staticmethod
    def from_yml(slug):
        fname = os.path.join(os.path.dirname(__file__), slug)
        with open(f"{fname}.yml") as f:
            return Strategy.from_data(yaml.safe_load(f.read()))

    @staticmethod
    def from_data(data):
        strategy = Strategy()

        strategy._id = data["id"]
        strategy._name = data["name"]
        strategy._slug = data["slug"]
        strategy._encoded = [
            [idx, encoded["name"], encoded["slug"], Type[encoded["type"]]]
            for idx, encoded in enumerate(data["encoded"])
        ]
        strategy._filterable = data["filterable"]
        strategy._sortable = data["sortable"]
        strategy._notes = data["notes"]
        strategy._category = Category[data["category"]]
        strategy._dials = data["dials"]
        strategy._special_sorts = data["special_sorts"]
        strategy._precedented_attrs = data["precedented_attrs"]

        return strategy

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def slug(self):
        return self._slug

    @property
    def encoded(self):
        return self._encoded

    @property
    def filterable(self):
        return self._filterable

    @property
    def sortable(self):
        return [
            {
                "slug": slug,
            }
            for slug in self._sortable
        ]

    @property
    def special_sorts(self):
        return self._special_sorts

    @property
    def precedented_attrs(self):
        return self._precedented_attrs

    @property
    def notes(self):
        return self._notes

    @property
    def category(self):
        return self._category

    @property
    def dials(self):
        return self._dials

    @property
    def mapping(self):
        return {
            no: {"name": col, "slug": slug} for no, col, slug, _type in self.encoded
        }

    @property
    def meta(self):
        attrs_slug_to_name = {
            slug: {"name": col, "type": _type.value}
            for no, col, slug, _type in self.encoded
        }

        return {
            "id": self.id,
            "name": self.name,
            "slug": self.slug,
            "attrs_slug_to_name": attrs_slug_to_name,
            "filterable": self.filterable,
            "sortable": self.sortable,
            "notes": self.notes,
            "category": self.category.value,
            "dials": self.dials,
            "special_sorts": self.special_sorts,
        }

    def csv_to_db_object(self, csvstring):
        sio = io.StringIO(csvstring)
        reader = csv.reader(sio)
        objs = []
        for i, row in enumerate(reader):
            if i == 0:
                continue
            obj = {
                self.mapping[i]["slug"]: Type.cast(
                    Type(
                        self.meta["attrs_slug_to_name"][self.mapping[i]["slug"]]["type"]
                    ),
                    val,
                )
                for i, val in enumerate(row)
                if i in self.mapping
            }
            objs.append({"symbol": obj["symbol"], "fundamentals": obj})
        return objs
