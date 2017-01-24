# -*- coding: utf-8 -*-
from nltk.tokenize import sent_tokenize, \
    word_tokenize  # 是NLTK提供的分词工具包. sent_tokenize() 函数对应的是分段为句。 word_tokenize()函数对应的是分句为词。
from nltk.corpus import stopwords  # 一个列表，包含了英文中那些频繁出现的词，如am, is, are
from collections import defaultdict  # 一个带有默认值的字典容器
from string import punctuation  # 一个列表，包含了英文中的标点和符号
from heapq import nlargest  # 函数可以很快地求出一个容器中最大的n个数字
import math
from itertools import product, count

stopwords = set(stopwords.words('english') + list(punctuation))
max_cut = 0.9
min_cut = 0.1

"""
计算出每个词出现的频率
word_sent 是一个已经分好词的列表
返回一个词典freq[],
freq[w]代表了w出现的频率
"""


def compute_frequencies(word_sent):
    """
    defaultdict和普通的dict
    的区别是它可以设置default值
    参数是int默认值是0
    """
    freq = defaultdict(int)

    for s in word_sent:
        for word in s:
            if word not in stopwords:
                freq[word] += 1

    m = float(max(freq.values()))

    for k, v in freq.items():
        freq[k] = v / m
        if freq[k] >= max_cut or freq[k] <= min_cut:
            del freq[k]

    return freq


def summarize(text, n):
    sents = sent_tokenize(text)
    assert n <= len(sents)

    word_sent = [word_tokenize(s.lower()) for s in sents]

    freq = compute_frequencies(word_sent)
    ranking = defaultdict(int)

    for i, word in enumerate(word_sent):
        for w in word:
            if w in freq:
                ranking[i] += freq[w]
    sent_idx = rank(ranking, n)
    return [sents[j] for j in sent_idx]


def rank(ranking, n):
    return nlargest(n, ranking, key=ranking.get)


"""
传入两个句子
返回这两个句子的相似度
"""


def calculate_similarity(sent1, sent2):
    counter = 0
    for word in sent1:
        if word in sent2:
            counter += 1
    return counter / (math.log(len(sent1)) + math.log(len(sent2)))


"""
传入句子的列表
返回各个句子之间相似度的图
（邻接矩阵表示）
"""


def create_graph(word_sent):
    num = len(word_sent)
    board = [[0.0 for _ in range(num)] for _ in range(num)]

    for i, j in product(range(num), repeat=2):
        if i != j:
            board[i][j] = calculate_similarity(word_sent[i], word_sent[j])
    return board


"""
判断前后分数有没有变化
这里认为前后差距小于0.0001
分数就趋于稳定
"""


def different(scores, old_scores):
    flag = False;
    for i in range(len(scores)):
        if math.fabs(scores[i] - old_scores[i]) >= 0.0001:
            flag = True
            break
    return flag


"""
根据公式求出指定句子的分数
"""


def calculate_score(weight_graph, scores, i):
    length = len(weight_graph)
    d = 0.85
    add_score = 0.0

    for j in range(length):
        denominator = 0.0

        fraction = weight_graph[j][i] * scores[j]
        for k in range(length):
            denominator += weight_graph[j][k]
        add_score += fraction / denominator

    weighted_score = (1 - d) + d * add_score

    return weighted_score


"""
输入相似度邻接矩阵
返回各个句子的分数
"""


def weighted_pagerank(weight_graph):
    scores = [0.5 for _ in range(len(weight_graph))]
    old_scores = [0.0 for _ in range(len(weight_graph))]

    while different(scores, old_scores):
        for i in range(len(weight_graph)):
            old_scores[i] = scores[i]

        for i in range(len(weight_graph)):
            scores[i] = calculate_score(weight_graph, scores, i)
    return scores


"""
使用TextRank
"""


def Summarize(text, n):
    # 首先分出句子
    sents = sent_tokenize(text)

    # 然后分出单词
    # word_sent是一个二维的列表
    # word_sent[i]代表的是第i句
    # word_sent[i][j]代表的是
    # 第i句中的第j个单词
    word_sent = [word_tokenize(s.lower()) for s in sents]

    # 把停用词去除
    for i in range(len(word_sent)):
        for word in word_sent[i]:
            if word in stopwords:
                word_sent[i].remove(word)

    similarity_graph = create_graph(word_sent)
    scores = weighted_pagerank(similarity_graph)
    sent_selented = nlargest(n, zip(scores, count()))
    sent_index = []
    for i in range(n):
        sent_index.append(sent_selented[i][1])

    return [sents[i] for i in sent_index]


if __name__ == '__main__':
    with open('news.txt', 'r') as myfile:
        text = myfile.read().replace('\n', '')
    # res = summarize(text, 2)¬
    # for i in range(len(res)):
    #     print(res[i])
    print(Summarize(text, 2))
