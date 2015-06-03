jQuery(function($, undefined) {
	var term = $('#terminal')
			.terminal(
					function(command, term) {
						if (command !== '') {
							term.pause();
							$
									.get(
											"/api/" + encodeURI(command),
											function(result) {
												try {
													var asJson = JSON
															.parse(result);
													var list = asJson["table"];
												} catch (e) {

												}
												if (list != undefined) {
													if (list.length == 0)
														term.echo("No Records");
													else {
														term.echo(list.length
																+ " records.");
														var first = true;
														var table = "<table class=\"compact\" >";
														for ( var index in list) {

															var obj = list[index];
															if (first) {
																first = false;
																table += "<thead> <tr>"
																for ( var prop in obj)
																	table += "<td>"
																			+ prop
																			+ "</td>";
																table += "</tr></thead>"
															}
															table += "<tr>";
															for ( var prop in obj)
																if (obj
																		.hasOwnProperty(prop))
																	table += "<td title=\""
																			+ obj[prop]
																			+ "\">"
																			+ obj[prop]
																			+ "</td>"
															table += "</tr>";
														}
														table += "</table>";
														term
																.echo(
																		table,
																		{
																			raw : true,
																			finalize : function(
																					div) {

																				div
																						.css(
																								"background-color",
																								"#969696");
																				div
																						.css(
																								"color",
																								"#000");
																				div
																						.find(
																								"table")
																						.DataTable(
																								{
																									"autoWidth" : true,
																									"scrollX" : true,
																									"scrollCollapse" : true
																								});
																				$(
																						div
																								.find("input"))
																						.on(
																								"click",
																								function() {
																									term
																											.focus(false);
																								});

																			}
																		});

													}
												} else
													term
															.echo(new String(
																	result));
												term.resume();
												$(document).scrollTop(
														$(document).height())
											}).fail(
											function(error) {
												term.error(error.responseText);
												term.resume();
												$(document).scrollTop(
														$(document).height())

											});
						} else {
							term.echo('');
						}
					},
					{
						greetings : 'TwitterStore\n Examples: store.getTweets(UID, TweetType.TWEETS/FAVORITES)'
								+ '\n store.getAdjacency(UID, ListType.FOLLOWEES/FOLLOWEERS)',
						name : 'TwitterStore Terminal',
						prompt : 'groovy > ',
						height : "100%"
					});
	$(document).on("click", function() {
		term.focus(true);
	})
});
$(document).tooltip();
/*
 * $(window).resize(function() { $('#terminal').height($(window).height() - 46);
 * });
 * 
 * $(window).trigger('resize');
 */