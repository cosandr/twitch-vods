import json
import os
import re
from typing import List

import cv2

from .utils import crop_to_regions


class Config:
    def __init__(self, **kwargs):
        args_opts = kwargs.pop('pattern_opt', [])
        if isinstance(args_opts, str):
            args_opts = [args_opts]
        opts = [getattr(re, name) for name in args_opts]
        self.re: re.Pattern = re.compile(kwargs.pop('pattern'), *opts)

        image_file: str = kwargs.pop('image')
        if not os.path.exists(image_file):
            raise FileNotFoundError(f'{image_file}: No such file')
        self.image = cv2.imread(image_file, cv2.IMREAD_GRAYSCALE)
        self.check_areas = kwargs.pop('check_areas')
        self.regions = crop_to_regions(self.image, self.check_areas)

    @classmethod
    def from_dict(cls, data: dict):
        if not data:
            return None
        return cls(**data)

    @classmethod
    def from_json(cls, file_path: str):
        """Return a list of Config instances by reading JSON file"""
        with open(file_path, 'r') as fr:
            config_dicts: List[dict] = json.load(fr)
        ret: List[Config] = []
        file_dir = os.path.dirname(file_path)
        for d in config_dicts:
            # Assume the path is relative if it doesn't exist
            if not os.path.exists(d['image']):
                d['image'] = os.path.join(file_dir, d['image'])
            ret.append(cls(**d))
        return ret
