package nimbus.client;

import java.io.IOException;

public class CacheletNotConnectedException extends IOException {
	private static final long serialVersionUID = 638860435155608907L;

	@Override
	public String getMessage() {
		return "Cachelet is not connected";
	}
}
