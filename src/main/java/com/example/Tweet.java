import java.io.Serializable;

public class Tweet implements Serializable {
	private String id;
	private String lang;
	private String text;
	private String name;
	private String source;
	private String created_at;

	public String getid() {
		return this.id;
	}

	public void setid(String value) {
		this.id = value;
	}

	public String getlang() {
		return this.lang;
	}

	public void setlang(String value) {
		this.lang = value;
	}

	public String gettext() {
		return this.text;
	}

	public void settext(String value) {
		this.text = value;
	}

	public String getname() {
		return this.name;
	}

	public void setname(String value) {
		this.name = value;
	}

	public String getsource() {
		return this.source;
	}

	public void setsource(String value) {
		this.source = value;
	}

	public String getcreated_at() {
		return this.created_at;
	}

	public void setcreated_at(String value) {
		this.created_at = value;
	}
}
