from generators.TBOXGenerator import TBOXGenerator
from generators.ABOXGenerator import ABOXGenerator
import os
import os.path as op


def main():

    BASEURL = "https://SDM.org/Lab2"
    output_dir = op.join(os.getcwd(), 'output')

    TBOXGenerator(BASEURL, op.join(output_dir, 'TBOX.ttl'))
    ABOXGenerator(BASEURL, op.join(output_dir, 'ABOX.ttl'))

    return None

if __name__ == '__main__':
    main()