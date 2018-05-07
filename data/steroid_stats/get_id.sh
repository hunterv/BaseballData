cat players | while read p
do
echo "grep '$p' ../clean/2000/players"
#grep --regexp=\'$p\' ../clean/1990/players

done 
