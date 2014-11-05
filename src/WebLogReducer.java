import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WebLogReducer extends
		Reducer<WebLogKey, WebLogVal, NullWritable, Text> {

	Long sessionExpire;

	List<Long> sessionTime;
	List<Integer> sessionActions;

	Set<String> actions;

	Long maxTimeStamp = Long.MIN_VALUE;
	Long minTimeStamp = Long.MAX_VALUE;

	Long prevUserId;
	Long prevTimeStamp;
	Text result;

	Configuration conf;
	String fieldTerminator;

	Long timeSpentByUser;
	Integer actionsCount;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		conf = context.getConfiguration();
		sessionTime = new LinkedList<Long>();
		sessionActions = new LinkedList<Integer>();
		actions = new HashSet<String>();

		prevUserId = 0L;
		prevTimeStamp = 0L;
		result = new Text();
		fieldTerminator = conf.get(WebLogConstants.fieldTerminatorProp);
		sessionExpire = conf.getLong(WebLogConstants.sessionExpireTimeProp, WebLogConstants.sessionExpire);
	}

	private void createSession() {
		sessionTime.add(maxTimeStamp - minTimeStamp);
		sessionActions.add(actions.size());
		actions.clear();
	}

	private String emitRec(String userId) {
		timeSpentByUser = 0L;
		actionsCount = 0;

		for (Long val : sessionTime)
			timeSpentByUser += val;

		for (Integer val : sessionActions)
			actionsCount += val;

		return userId
				+ fieldTerminator
				+ (sessionTime.size() > 0 ? timeSpentByUser
						/ sessionTime.size() : "")
				+ fieldTerminator
				+ (sessionActions.size() > 0 ? actionsCount
						/ sessionActions.size() : "");
	}

	@Override
	protected void reduce(WebLogKey key, Iterable<WebLogVal> vals,
			Context context) throws IOException, InterruptedException {

		if (prevUserId != 0L && key.getUserId().get() != prevUserId) {
			// Emit Record
			minTimeStamp = prevTimeStamp;
			createSession();
			result.set(emitRec(String.valueOf(prevUserId)));
			context.write(NullWritable.get(), result);
			sessionActions.clear();
			sessionTime.clear();
			maxTimeStamp = key.getTimeStamp().get();
		}

		if (prevUserId == 0L)
			maxTimeStamp = key.getTimeStamp().get();

		if (prevTimeStamp > key.getTimeStamp().get() + sessionExpire) {
			// Emit Session
			minTimeStamp = prevTimeStamp;
			createSession();
			maxTimeStamp = key.getTimeStamp().get();
		}

		for (WebLogVal val : vals)
			actions.add(val.getAction().toString());

		prevUserId = key.getUserId().get();
		prevTimeStamp = key.getTimeStamp().get();
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		minTimeStamp = prevTimeStamp;
		createSession();
		/* Getting the results in emitRec 
		   1) The average session duration
		   2) The average number of actions per session 
		 */

		result.set(emitRec(String.valueOf(prevUserId)));
		context.write(NullWritable.get(), result);
	}
}
