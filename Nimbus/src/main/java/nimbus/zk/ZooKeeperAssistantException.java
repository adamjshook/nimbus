package nimbus.zk;

public class ZooKeeperAssistantException extends RuntimeException {

	private static final long serialVersionUID = -6622677076486957557L;
	private String msg = null;

	public ZooKeeperAssistantException() {
		msg = "ZooKeeperAssistantException: not message given";
	}

	public ZooKeeperAssistantException(String msg) {
		msg = "ZooKeeperAssistantException: " + msg;
	}

	public ZooKeeperAssistantException(Exception e) {
		msg = "ZooKeeperAssistantException: " + e.getMessage();
	}

	@Override
	public String getMessage() {
		return msg;
	}
}
