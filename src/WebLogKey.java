 import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class WebLogKey implements WritableComparable<WebLogKey> {
	private LongWritable userId;
	private LongWritable timeStamp;

	public WebLogKey()
	{
		this.userId = new LongWritable();
		this.timeStamp = new LongWritable();
	}

	public WebLogKey(Long userId, Long timeStamp) {
		this.userId = new LongWritable(userId);
		this.timeStamp = new LongWritable(timeStamp);
	}

	public WebLogKey(LongWritable userId, LongWritable timeStamp) {
		this.userId = userId;
		this.timeStamp = timeStamp;
	}

	public void set (String userId, String timeStamp) {
		this.userId.set(Long.parseLong(userId));
		this.timeStamp.set(Long.parseLong(timeStamp));
	}

	public LongWritable getUserId() {
		return userId;
	}

	public void setUserId(LongWritable userId) {
		this.userId = userId;
	}

	public void setUserId(Long userId) {
		this.userId.set(userId);
	}

	public LongWritable getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(LongWritable timeStamp) {
		this.timeStamp = timeStamp;
	}

	public void setTimeStamp(Long timeStamp) {
		this.timeStamp.set(timeStamp);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		userId.readFields(in);
		timeStamp.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		userId.write(out);
		timeStamp.write(out);
	}

/* Generating hashcode based on userID */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((userId == null) ? 0 : userId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WebLogKey other = (WebLogKey) obj;
		if (timeStamp == null) {
			if (other.timeStamp != null)
				return false;
		} else if (!timeStamp.equals(other.timeStamp))
			return false;
		if (userId == null) {
			if (other.userId != null)
				return false;
		} else if (!userId.equals(other.userId))
			return false;
		return true;
	}
/* Return the keys in reverse order (i.e. latest timestamp at the top) */
	@Override
	public int compareTo(WebLogKey o) {
		int comp = this.userId.compareTo(o.userId);
		if (comp != 0)
			return comp;
		comp = -1 * this.timeStamp.compareTo(o.timeStamp);
		return comp;
	}

	@Override
	public String toString() {
		return "WebLogKey [userId=" + userId + ", timeStamp=" + timeStamp + "]";
	}
}
