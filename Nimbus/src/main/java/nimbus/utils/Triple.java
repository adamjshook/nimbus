package nimbus.utils;

public class Triple {

	private String first = null;
	private String second = null;
	private String third = null;

	public Triple() {
	}

	public Triple(String first, String second, String third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public String getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second = second;
	}

	public String getThird() {
		return third;
	}

	public void setThird(String third) {
		this.third = third;
	}

	public void set(Triple t) {
		first = t.getFirst();
		second = t.getSecond();
		third = t.getThird();
	}

	public void set(String f, String s, String t) {
		first = f;
		second = s;
		third = t;
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return obj.toString().equals(this.toString());
	}

	@Override
	public String toString() {
		return (getFirst() + getSecond() + getThird());
	}
}
