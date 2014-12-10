package isistan.twitter.crawler.store.h2;

import java.io.Serializable;

public class Reply implements Serializable {

	public long inreplytouid;
	public long inreplytostatusid;
	public String inreplytoscn;

	public Reply(long inReplyToUserId, String inReplyToScreenName,
			long inReplyToStatusId) {
		this.inreplytouid = inReplyToUserId;
		this.inreplytostatusid = inReplyToStatusId;
		this.inreplytoscn = inReplyToScreenName;
	}

	@Override
	public String toString() {
		return "Reply [inreplytoid=" + inreplytouid + ", inreplytostatusid="
				+ inreplytostatusid + ", inreplytoscn=" + inreplytoscn + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (inreplytouid ^ (inreplytouid >>> 32));
		result = prime * result
				+ ((inreplytoscn == null) ? 0 : inreplytoscn.hashCode());
		result = prime * result
				+ (int) (inreplytostatusid ^ (inreplytostatusid >>> 32));
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
		Reply other = (Reply) obj;
		if (inreplytouid != other.inreplytouid)
			return false;
		if (inreplytoscn == null) {
			if (other.inreplytoscn != null)
				return false;
		} else if (!inreplytoscn.equals(other.inreplytoscn))
			return false;
		if (inreplytostatusid != other.inreplytostatusid)
			return false;
		return true;
	}

}
