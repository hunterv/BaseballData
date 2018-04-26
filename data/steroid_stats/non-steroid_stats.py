#!/bin/python

#import os
import re
import sys
import glob

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: ./steriod_stats <user_ids_file> <plays for decade>")
        exit(0)
    infile = sys.argv[1]
    play_files = sys.argv[2]

    steroid_players = {}

    # Iterate over each file (can use wildcards)
    print("Starting main iteration")
    with open(infile, 'r') as f: # file being read with player ids
        for line in f:
            line = line.rstrip()
            steroid_players[line] = True 

    with open("nonuser-stats", "a") as out: # File written to with plays
        for filename in glob.glob(play_files): # Set of files for decade
            with open(filename, 'r') as plays: # Single file with plays
                for play in plays:
                    player_list = play.split("\s+")[0]
                    player_id = player_list.split("~")[0]
                    if player_id not in steroid_players:
                        out.write(play) # Write the whole line to our outfile

            plays.close() # Close that play file once it's done

        # Close all files 
        out.close()
        f.close()
                

