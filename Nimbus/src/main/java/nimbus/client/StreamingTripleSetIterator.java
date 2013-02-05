package nimbus.client;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import nimbus.server.TripleSetCacheletWorker;
import nimbus.utils.Triple;

public class StreamingTripleSetIterator implements Iterator<Triple> {

	private List<BaseNimbusClient> connect = null;
	private Triple currentTriple = new Triple();
	private Triple nextTriple = new Triple();
	private String response = null;
	private String[] responseTokens = new String[3];
	private boolean notDone = true;
	private int index = 0;

	public void addClient(BaseNimbusClient connect) throws IOException {
		this.connect.add(connect);
	}

	public void initialize() {
		advanceTriple();
	}

	@Override
	public boolean hasNext() {
		return notDone;
	}

	@Override
	public Triple next() {
		if (notDone) {
			nextTriple.set(currentTriple);
			advanceTriple();
			return nextTriple;
		} else {
			return null;
		}
	}

	private void advanceTriple() {
		do {
			try {
				response = connect.get(index).readLine();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			if (response.equals(TripleSetCacheletWorker.EOS)) {
				++index;
				if (index == connect.size()) {
					notDone = false;
				}
			} else {
				responseTokens = response.split(" ");
				currentTriple.setFirst(responseTokens[0]);
				currentTriple.setSecond(responseTokens[1]);
				currentTriple.setThird(responseTokens[2]);
				break;
			}
		} while (true);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Cannot remove using iterator");
	}
}
