# -*- coding: utf-8 -*-
import markdown
from bs4 import BeautifulSoup
import os
import sys


class MarkdownToHtml:
    headTag = '<head><meta charset="utf-8" /></head>'

    def __init__(self, cssFile=None):
        if cssFile != None:
            self.genStyle(cssFile)

    def genStyle(self, cssFile):
        with open(cssFile, 'r') as f:
            cssString = f.read()
        self.headTag = self.headTag[:-7] + '<style type="text/css">{}</style>'.format(cssString) + self.headTag[-7:]

    # os.path.abspath()    将参数路径转为绝对路径并返回
    # os.path.dirname()    获得参数路径的目录部分并返回（如"\home\a.txt"为参数，返回"\home"）
    # os.path.basename()    返回参数路径字符串中的完整文件名（文件名 + 后缀名）
    # os.path.splitext()    将参数转换为包含文件名和后缀名两个元素的元组并返回
    def markdownToHtml(self, sourceFilePath, destinationDirectory=None, outputFileName=None):
        if not destinationDirectory:
            destinationDirectory = os.path.dirname(os.path.abspath(sourceFilePath))
        if not outputFileName:
            outputFileName = os.path.splitext(os.path.basename(sourceFilePath))[0] + '.html'
        if destinationDirectory[-1] != '/':
            destinationDirectory += '/'
        with open(sourceFilePath, 'r') as f:
            markdownText = f.read().decode('utf8')
        rawHtml = self.headTag + markdown.markdown(markdownText, output_format='html5')
        beautifyHtml = BeautifulSoup(rawHtml, 'html5lib').prettify()
        with open(destinationDirectory + outputFileName, 'w') as f:
            s = beautifyHtml.encode('utf8')
            f.write(s)

    def markdownFileToHtml(self, sourceFilePath):
        markdownText = sourceFilePath.read().decode('utf8')
        rawHtml = self.headTag + markdown.markdown(markdownText, output_format='html5')
        beautifyHtml = BeautifulSoup(rawHtml, 'html5lib').prettify()
        s = beautifyHtml.encode('utf8')
        return s


if __name__ == "__main__":
    mth = MarkdownToHtml()
    # 做一个命令行参数列表的浅拷贝，不包含脚本文件名
    argv = sys.argv[1:]
    # 目前列表 argv 可能包含源文件路径之外的元素（即选项信息）
    # 程序最后遍历列表 argv 进行编译 markdown 时，列表中的元素必须全部是源文件路径
    outputDirectory = None
    if '-s' in argv:
        cssArgIndex = argv.index('-s') + 1
        cssFilePath = argv[cssArgIndex]
        # 检测样式表文件路径是否有效
        if not os.path.isfile(cssFilePath):
            print('Invalid Path: ' + cssFilePath)
            sys.exit()
        mth.genStyle(cssFilePath)
        # pop 顺序不能随意变化
        argv.pop(cssArgIndex)
        argv.pop(cssArgIndex - 1)
    if '-o' in argv:
        dirArgIndex = argv.index('-o') + 1
        outputDirectory = argv[dirArgIndex]
        # 检测输出目录是否有效
        if not os.path.isdir(outputDirectory):
            print('Invalid Directory: ' + outputDirectory)
            sys.exit()
        # pop 顺序不能随意变化
        argv.pop(dirArgIndex)
        argv.pop(dirArgIndex - 1)
    # 至此，列表 argv 中的元素均是源文件路径
    # 遍历所有源文件路径
    for filePath in argv:
        # 判断文件路径是否有效
        if os.path.isfile(filePath):
            mth.markdownToHtml(filePath, outputDirectory)
        else:
            print('Invalid Path: ' + filePath)
