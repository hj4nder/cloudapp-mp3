import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import clojure.lang.Obj;

import java.util.HashMap;
import java.util.Objects;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
  private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();
  private int N;

  private long intervalToReport = 20;
  private long lastReportTime = System.currentTimeMillis();


    private int lastMinCount = 0;
    private String lastWordKey;

  public TopNFinderBolt(int N) {
    this.N = N;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
 /*
    ----------------------TODO-----------------------
    Task: keep track of the top N words
    ------------------------------------------------- */

      String word = tuple.getStringByField("word");

      Integer count = tuple.getIntegerByField("count");

      currentTopWords.put(word, count);

      if (currentTopWords.size() > N) {
          Integer smallestWordCount = null;
          String smallestWord = null;

          for (String keywords : currentTopWords.keySet()) {
              if (smallestWordCount == null) {
                  smallestWordCount = currentTopWords.get(keywords);
                  smallestWord = keywords;
              } else if (currentTopWords.get(keywords) < smallestWordCount) {
                  smallestWordCount = currentTopWords.get(keywords);
                  smallestWord = keywords;
              }
          }

          currentTopWords.remove(smallestWord);
      }


    //reports the top N words periodically
    if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
      collector.emit(new Values(printMap()));
      lastReportTime = System.currentTimeMillis();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

     declarer.declare(new Fields("top-N"));

  }

  public String printMap() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("top-words = [ ");
    for (String word : currentTopWords.keySet()) {
      stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
    }
    int lastCommaIndex = stringBuilder.lastIndexOf(",");
    stringBuilder.deleteCharAt(lastCommaIndex + 1);
    stringBuilder.deleteCharAt(lastCommaIndex);
    stringBuilder.append("]");
    return stringBuilder.toString();

  }
}
