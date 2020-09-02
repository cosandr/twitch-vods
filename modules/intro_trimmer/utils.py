import numpy as np
from typing import List, Dict


def crop_to_regions(img: np.ndarray, check_areas: List[Dict[str, List[int]]]) -> List[np.ndarray]:
    """Returns regions defined by check_areas"""
    ret = []
    for region in check_areas:
        from_x = region['start'][0]
        from_y = region['start'][1]
        to_x = from_x + region['size'][0]
        to_y = from_y + region['size'][1]
        # Don't overflow
        to_x = to_x if to_x < img.shape[1] else img.shape[1] - 1
        to_y = to_y if to_y < img.shape[0] else img.shape[0] - 1
        ret.append(img[from_y:to_y, from_x:to_x])
    return ret
