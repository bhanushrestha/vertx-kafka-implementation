/**
 * 
 */
package vertx.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * @author prayas
 *
 */
public class SimplePartitioner implements Partitioner {
    public int partition(Object key, int numPartitions) {
        int partition = 0;
       String keyS = (String) key;
        int iKey = Integer.parseInt(keyS);
        if (iKey > 0) {
           partition = iKey % numPartitions;
        }
       return partition;
  }
    
    public SimplePartitioner (VerifiableProperties props) {
    	 
    }

}
