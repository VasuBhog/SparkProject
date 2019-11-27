declare -a temp=("weather/2010temp" "weather/2011temp" "weather/2012temp" "weather/2013temp" "weather/2014temp" "weather/2015temp" "weather/2016temp" "weather/2017temp" "weather/2018temp" "weather/2019temp")
files=$(echo $(hadoop fs -ls weather | sed 1d | perl -wlne'print +(split " ",$_,8)[7]'))

echo $files
for i in "${temp[@]}"
do
  echo $i 
  if [[ "$files" =~ $i ]]; then
    echo "FOUND TEMP"
    echo $(hadoop fs -rm -r "$i")
  fi
done