usage() {
  echo -e "Usage: $0 [-i1 <path>] [-i2 <path>] [-o <path>]\n"\
       "where\n"\
       "-i1 defines an input path\n"\
       "-i2 defines an input path\n"\
       "-o defines an output path\n"\
       "-m defines max output size"
       "-e defines an executor: hadoop or yarn, yarn but default\n"\
       "\n"\
        1>&2
  exit 1
}


while getopts ":i1:i2:o:m:e:" opt; do
    case "$opt" in
        i1) INPUT_PATH1=${OPTARG} ;;
        i2) INPUT_PATH2=${OPTARG} ;;
        o)  OUTPUT_PUTH=${OPTARG} ;;
        m)  OUTPUT_SIZE=${OPTARG} ;;
        e)  EXECUTOR=${OPTARG} ;;
        *)  usage ;;
    esac
done

if [[ -z "$INPUT_PATH1" ]];
then
  INPUT_PATH1="/bdpc/Avg_Delays_MR/Avg_delays/input/airlines.csv"
fi

if [[ -z "$INPUT_PATH2" ]];
then
  INPUT_PATH2="/bdpc/Avg_Delays_MR/Avg_delays/input/flights.csv"
fi

if [[ -z "$OUTPUT_PATH" ]];
then
  OUTPUT_PATH="/bdpc/hadoop_mr/avg_delay/output"
fi

if [[ -z "$OUTPUT_SIZE" ]];
then
  OUTPUT_SIZE="5"
fi

if [[ -z "$EXECUTOR" ]];
then
  EXECUTOR="yarn"
fi

hadoop fs -rm -R $OUTPUT_PATH
hadoop fs -rm -R "/bdpc/hadoop_mr/avg_delay/outputjoin"
hdfs dfs -ls ${INPUT_PATH}

THIS_FILE=$(readlink -f "$0")
THIS_PATH=$(dirname "$THIS_FILE")
BASE_PATH=$(readlink -f "$THIS_PATH/../")
APP_PATH="$THIS_PATH/avg_delays-1.0-jar-with-dependencies.jar"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo "THIS_FILE = $THIS_FILE"
echo "THIS_PATH = $THIS_PATH"
echo "BASE_PATH = $BASE_PATH"
echo "APP_PATH = $APP_PATH"
echo "-------------------------------------"
echo "INPUT_PATH1 = $INPUT_PATH1"
echo "INPUT_PATH2 = $INPUT_PATH2"
echo "OUTPUT_PUTH = $OUTPUT_PATH"
echo "OUTPUT_SIZE = $OUTPUT_SIZE"
echo "-------------------------------------"

mapReduceArguments=(
  "$APP_PATH"
  "com.globallogic.bdpc.mapreduce.avgdelays.AverageDriver"
  "$INPUT_PATH1"
  "$INPUT_PATH2"
  "$OUTPUT_PATH"
  "$OUTPUT_SIZE"
)

SUBMIT_CMD="${EXECUTOR} jar ${mapReduceArguments[@]}"
echo "$SUBMIT_CMD"
${SUBMIT_CMD}

echo "You should find results here: 'hadoop fs -ls $OUTPUT_PATH'"
echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
