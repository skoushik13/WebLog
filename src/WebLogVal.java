import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class WebLogVal implements Writable {
	private Text action;

	public WebLogVal ()
	{
		this.action = new Text();
	}

	public WebLogVal(Text action)
	{
		this.action = action;
	}

	public WebLogVal(String action)
	{
		this.action = new Text(action);
	}

	public Text getAction() {
		return action;
	}

	public void setAction(Text action) {
		this.action = action;
	}

	public void setAction(String action) {
		this.action.set(action);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		action.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		action.write(out);
	}

	@Override
	public String toString() {
		return "WebLogVal [Action=" + action + "]";
	}

}
