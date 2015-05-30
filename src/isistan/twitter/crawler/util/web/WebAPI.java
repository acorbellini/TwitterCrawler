package isistan.twitter.crawler.util.web;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

@Path("api")
public class WebAPI {

	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/{expr}")
	public Response expr(@PathParam("expr") String expr) {
		String ret;
		try {
			Object val = Shell.getShell().evaluate(expr);
			ret = "";
			if (val != null) {
				if (List.class.isAssignableFrom(val.getClass())) {
					List asList = (List) val;
					StringBuilder builder = new StringBuilder();
					builder.append("{\"table\":[");
					boolean first = true;
					for (Object object : asList) {
						if (first) {
							first = false;
						} else
							builder.append(",");
						builder.append(object.toString() + "\n");
					}
					builder.append("]}");
					ret = builder.toString();
				} else if (val.getClass().isArray()) {

					JsonArray array = new JsonArray();
					boolean first = true;
					for (int i = 0; i < Array.getLength(val); i++) {
						JsonObject obj = new JsonObject();
						obj.addProperty("pos", i);
						obj.addProperty("el", Array.get(val, i).toString());
						array.add(obj);
					}
					ret = "{\"table\":" + array.toString() + "}";
				} else
					ret = val.toString();
			}
			return Response.status(200).entity(ret).build();
		} catch (Exception e) {
			e.printStackTrace();
			return Response.status(500).entity(e.getMessage()).build();
		}
	}
}
