#!/bin/python
# INTENDED TO BE RUN ONE DECADE AT A TIME FROM INSIDE THAT DIR!

#import os
import re
import sys
import glob

def print_players(filename, players):
    with open("fixed/players", 'w') as f:
        for p in sorted(players):
            f.write(p + ": " + players[p] + "\n")
    f.close()
    return 0

def extract_play_info(play_line):
    components = play_line.split(',')
    player_id = components[3]
    play = components[6]
    return (player_id, play)

def get_player(player_line):
    components = player_line.split(',')
    code = components[1]
    name = components[2]
    return (code, name)

def print_player_plays(plays):
    for player in plays:
        with open("fixed/plays", 'a') as out:
            for p in plays[player]:
                out.write(player + ": " + p) # Newline already in file, don't add
        out.close()
    return 0

if __name__ == "__main__":
    infile = sys.argv[1]
    print("INPUT: " + infile)

    # Storing data for later (hopefully fewer writes overall)
    players = {}
    player_plays = {}

    # Some useful regex pre-compiled
    id_line = re.compile("^id")
    info_line = re.compile("^info")
    start_line = re.compile("^start")
    sub_line = re.compile("^sub")
    play_line = re.compile("^play")

    # Iterate over each file (can use wildcards)
    print("Starting main iteration")
    for filename in glob.glob(infile):
        print("Working on " + filename)
        with open(filename, 'r') as f: # Opens the single file we're editing at the moment
            with open("fixed/" + "clean_" + filename , "a") as out:
                for line in f:
                    # First the lines we just write though
                    # Split so we can change rules later
                    if re.match(id_line, line):
                        out.write(line) 
                    if re.match(info_line, line):
                        out.write(line)
                    
                    # Now we handle the player stuff
                    if re.match(start_line, line) or re.match(sub_line, line):
                        code, name = get_player(line)
                        players[code] = name

                    # Then finally handle plays (written though, but also stored)
                    if re.match(play_line, line):
                        player_id, play = extract_play_info(line)
                        if player_id in player_plays:
                            # Confirm we're working with a list here
                            if type(player_plays[player_id]) != type([]):
                                player_plays[player_id] = []

                            # Add play to that players list
                            player_plays[player_id].append(play)
                        else:
                            player_plays[player_id] = []

                            player_plays[player_id].append(play)

                        # Finally write through to file
                        out.write(line)

                # Close all files before looping again
                out.close()
                f.close()
                
    # Output stored data
    print("Printing player directory")
    print_players("fixed/players", players)

    print("Printing play list")
    print_player_plays(player_plays)

