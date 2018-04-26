import sys
with open(sys.argv[1], 'r') as infile:
    
    s = [line for line in infile]
    b = []
    for i in range(len(s)):
        b.append(s[1])
        if i%3 == 0:
            #write to csv file
            b = []
    
