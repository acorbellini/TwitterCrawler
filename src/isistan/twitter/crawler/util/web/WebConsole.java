package isistan.twitter.crawler.util.web;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("")
public class WebConsole {

	@GET
	@Produces(MediaType.TEXT_HTML)
	@Path("/")
	public Response index() throws IOException {
		return web("");
	}

	@GET
	@Produces(MediaType.WILDCARD)
	@Path("/{path:.*}")
	public Response web(@PathParam("path") String path) throws IOException {
		if (path == null || path.isEmpty())
			path = "index.html";
		InputStream is = getClass().getResourceAsStream(path);
		if (is == null)
			return Response.status(404).build();
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		byte[] buff = new byte[8 * 1024];
		int read = 0;
		while ((read = is.read(buff)) > 0) {
			bos.write(buff, 0, read);
		}
		return Response.status(200).entity(bos.toByteArray()).build();
	}
}
