# -*- coding: utf-8 -*-
from PIL import Image
import hashlib
import time
import os

from captcha.VectorCompare import VectorCompare


class Recognition(object):
    iconset = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
               'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']

    v = VectorCompare()
    im2 = None
    im = None

    # 加载训练集
    imageset = []

    # 将图片转换为矢量
    def buildvector(self, im):
        d1 = {}

        count = 0
        for i in im.getdata():
            d1[count] = i
            count += 1

        return d1

    def training_set(self):
        for letter in self.iconset:
            for img in os.listdir('./captcha/iconset/%s/' % letter):
                temp = []
                if img != "Thumbs.db" and img != ".DS_Store":  # windows check...
                    temp.append(self.buildvector(Image.open("./captcha/iconset/%s/%s" % (letter, img))))
                self.imageset.append({letter: temp})

    def input_image(self, im):

        # 灰度化输入图片
        # im = Image.open("captcha.gif")
        self.im2 = Image.new("P", im.size, 255)
        im.convert("P")
        temp = {}

        for x in range(im.size[1]):
            for y in range(im.size[0]):
                pix = im.getpixel((y, x))
                temp[pix] = pix
                if pix == 220 or pix == 227:  # 用220和227两个灰度的数据生成图像
                    self.im2.putpixel((y, x), 0)  # 用黑色生成新图像
        self.im = im

        self.training_set()

        self.labelClipImg()

        return self.recognition()

    inletter = False
    foundletter = False
    start = 0
    end = 0
    letters = []

    def labelClipImg(self):
        # 标记切割坐标
        for y in range(self.im2.size[0]):  # slice across
            for x in range(self.im2.size[1]):  # slice down
                pix = self.im2.getpixel((y, x))
                if pix != 255:
                    self.inletter = True

            if self.foundletter == False and self.inletter == True:
                self.foundletter = True
                start = y

            if self.foundletter == True and self.inletter == False:
                self.foundletter = False
                end = y
                self.letters.append((start, end))

            self.inletter = False

    def recognition(self):
        output = []
        count = 0
        for letter in self.letters:
            m = hashlib.md5()
            # 对验证码图片进行切割
            im3 = self.im2.crop((letter[0], 0, letter[1], self.im2.size[1]))
            guess = []
            for image in self.imageset:
                for x, y in image.iteritems():
                    if len(y) != 0:
                        guess.append((self.v.relation(y[0], self.buildvector(im3)), x))
            guess.sort(reverse=True)
            print "", guess[0]
            output.append(guess[0])
            count += 1
        return output
