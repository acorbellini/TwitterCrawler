package isistan.twitter.mysql;

public class Counter {
	int cont = 0;

	public Counter(int cont) {
		this.cont = cont;
	}

	public synchronized int getCont() {
		return cont;
	}

	public synchronized void incCont() {
		cont++;
	}

	public synchronized void incCont(int cant) {
		cont += cant;
	}
}