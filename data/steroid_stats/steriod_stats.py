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

    # Iterate over each file (can use wildcards)
    print("Starting main iteration")
    with open(infile, 'r') as f: # file being read with player ids
        with open("user-stats", "a") as out: # File written to with plays
            for player_id in f: # For each player
                for filename in glob.glob(play_files): # Set of files for decade
                    with open(filename, 'r') as plays: # Single file with plays
                        player_id = player_id.rstrip()
                        for play in plays: # For each play
                            if player_id in play:
                                out.write(play) # Write the whole line to our outfile

                plays.close() # Close that play file once it's done

        # Close all files 
        out.close()
        f.close()
                

