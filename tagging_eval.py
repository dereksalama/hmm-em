from __future__ import division
import pdb
import codecs
import sys
import re
from collections import defaultdict

def parse_tagging(file_name):
    inputf = codecs.open(file_name, 'r', 'utf8')

    total_tagging = []
    for line in inputf:
        # skip empties
        if len(line) <= 0:
            continue

        for pair in line.split():
            m = re.match(r"\((.*),(.*)\)", pair)
            tag = m.group(2)
            total_tagging.append(tag)

    return total_tagging

def create_tag_map(hmm_tags, gold_tags):
    maximum_map = dict()
    for tag in set(hmm_tags): # iterate through unique tags
        cooccurrence_count = defaultdict(int)

        for (hmm_t, gold_t) in zip(hmm_tags, gold_tags):
            if hmm_t == tag:
                cooccurrence_count[gold_t] += 1

        maximum_cooccurrance = max(cooccurrence_count.iterkeys(), 
                key=(lambda key: cooccurrence_count[key]))
        maximum_map[tag] = maximum_cooccurrance

    return maximum_map

def num_correct(hmm_to_gold, hmm_tags, gold_tags):
    correct = 0
    for (hmm_t, gold_t) in zip(hmm_tags, gold_tags):
        if hmm_to_gold[hmm_t] == gold_t:
            correct += 1

    return correct

if __name__=='__main__':
    hmm_output_file = sys.argv[1]
    gold_tagging_file = sys.argv[2]

    hmm_tags = parse_tagging(hmm_output_file)
    gold_tags = parse_tagging(gold_tagging_file)

    hmm_to_gold_tags = create_tag_map(hmm_tags, gold_tags)
    correct = num_correct(hmm_to_gold_tags, hmm_tags, gold_tags)

    print "PERCENT CORRECT: ",
    print correct / len(hmm_tags)

