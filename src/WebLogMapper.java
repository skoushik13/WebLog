import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebLogMapper extends Mapper<LongWritable, Text, WebLogKey, WebLogVal> {

	WebLogKey logKey;
	WebLogVal logVal;
	String fieldTerminator;
	String sessionTerminator;
	Configuration conf;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		conf = context.getConfiguration();
		logKey = new WebLogKey();
		logVal = new WebLogVal();
		fieldTerminator = conf.get(WebLogConstants.fieldTerminatorProp, WebLogConstants.fieldTerminator);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = StringUtils.splitPreserveAllTokens(value.toString(), fieldTerminator);
		
		/* setting key as timestamp and userID */
		logKey.set(fields[1], fields[0]);
		
		/* setting key as action */
		logVal.setAction(fields[2]);
		context.write(logKey, logVal);
	}
}
