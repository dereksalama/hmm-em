import re
import pdb
import codecs
import sys

# output format:
# (token,tag) (token, tag)....

def convert(input_file_name, output_file_name):
    inputf = codecs.open(input_file_name, 'r', 'utf8')
    outf = codecs.open(output_file_name, 'w', 'utf8')

    for line in inputf:
        if line is None:
            continue

        line = line.lstrip()
        line = line.rstrip()

        # skip inputf of '============='
        if len(line.replace("=", "")) == 0 and len(line) != 0:
            outf.write("\n")
            continue

        line = line.replace("[", "")
        line = line.replace("]", "")


        tokens = line.split()
        tokenPairs = map(lambda token: re.split(r"(?<!\\)/", token, 1), tokens)

        for tokenPair in tokenPairs:
            if tokenPair[0] != tokenPair[1] and re.sub("[^a-zA-Z0-9]", "", tokenPair[0]) != "":
                outf.write("(" + tokenPair[0] + ",")

                # for now, only choose the first tagging for evaluation
                states = re.split("\|", tokenPair[1])
                outf.write(states[0] + ") ")

    inputf.close()
    outf.close()

if __name__=='__main__':
    pos_file = sys.argv[1]
    output_file_name = sys.argv[2]

    convert(pos_file, output_file_name)
