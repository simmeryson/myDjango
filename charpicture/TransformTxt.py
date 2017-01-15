# -*- coding: utf-8 -*-
from PIL import Image

ascii_char = list("$@B%8&WM#*oahkbdpqwmZO0QLCJUYXzcvunxrjft/\|()1{}[]?-_+~<>i!lI;:,\"^`'. ")


class TransformTxt(object):
    WIDTH = 80
    HEIGHT = 80
    # 将256灰度映射到70个字符上
    def get_char(self, r, g, b, alpha=256):
        if alpha == 0:
            return ' '
        length = len(ascii_char)
        gray = int(0.2126 * r + 0.7152 * g + 0.0722 * b)

        unit = (256.0 + 1) / length
        return ascii_char[int(gray / unit)]

    def transform(self, img):
        im = Image.open(img)
        im = im.resize((self.WIDTH, self.HEIGHT), Image.NEAREST)

        txt = ""

        for i in range(self.HEIGHT):
            for j in range(self.WIDTH):
                txt += self.get_char(*im.getpixel((j, i)))
            txt += '\n'

        print txt
        return txt
