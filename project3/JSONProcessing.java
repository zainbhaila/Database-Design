import java.sql.*;
import java.util.*;
import org.json.simple.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONArray;

public class JSONProcessing
{
	public static void processJSON(String json) {
		try {
			JSONParser jp = new JSONParser(); // converting string to json object
			JSONObject input = (JSONObject) jp.parse(json);
			try {
					Class.forName("org.postgresql.Driver");
			} catch (ClassNotFoundException e) {
					System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
					e.printStackTrace();
					return;
			}
			Connection connection = null;
			try {
					connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/olympics","vagrant", "vagrant");
			} catch (SQLException e) {
					System.out.println("Connection Failed! Check output console");
					e.printStackTrace();
					return;
			}
			if (connection != null) {
			} else {
					System.out.println("Failed to make connection!");
					return;
			}
			Statement stmt = null;

			if (input.get("newevent") != null) {
				JSONObject inner = (JSONObject) input.get("newevent");
				String event_id = (String) inner.get("event_id");
				String name = (String) inner.get("name");
				String eventtype = (String) inner.get("eventtype");
				String olympic_id = (String) inner.get("olympic_id");
				int is_team_event = Integer.parseInt((String) inner.get("is_team_event"));
				int num_players_in_team = Integer.parseInt((String) inner.get("num_players_in_team"));
				String result_noted_in = (String) inner.get("result_noted_in");

        String query = "insert into events (event_id, name, eventtype, olympic_id, is_team_event, num_players_in_team, result_noted_in) values ('"+event_id+"','"+name+"','"+eventtype+"','"+olympic_id+"',"+is_team_event+","+num_players_in_team+",'"+result_noted_in+"');";
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate(query);
            stmt.close();
						System.out.println("Adding data from " + json + " into the database");
        } catch (SQLException e ) {
            System.out.println(e);
        }
			}
			else {
				JSONObject inner = (JSONObject) input.get("medalinformation");
				String event_id = (String) inner.get("event_id");
				JSONArray pm = (JSONArray) inner.get("players_and_medals");

				ArrayList<String> player_ids = new ArrayList<String>(); // store json object
				ArrayList<String> medals = new ArrayList<String>();
				ArrayList<Double> results = new ArrayList<Double>();

        Iterator itr = pm.iterator(); // iterate over all results for event
        while (itr.hasNext())
        {
            JSONObject temp = (JSONObject) itr.next();
						String player_id = (String) temp.get("player_id");
						String medal = (String) temp.get("medal");
						Double result = Double.parseDouble((String) temp.get("result"));

						player_ids.add(player_id);
						medals.add(medal);
						results.add(result);
        }

				int invalid_input = 0;
				String query;
				query = "select * from events where event_id = '"+event_id+"'";
        try {
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
						if (rs.next() == false) {
							invalid_input = 1;
							System.out.println("Invalid event id inputted.");
						}
            stmt.close();
        } catch (SQLException e ) {
            System.out.println(e);
						invalid_input = 1;
        }
				for (String i : player_ids) {
					query = "select * from players where player_id = '"+i+"'";
	        try {
	            stmt = connection.createStatement();
	            ResultSet rs = stmt.executeQuery(query);
							if (rs.next() == false) {
								invalid_input = 1;
								System.out.println("Invalid player id inputted.");
							}
	            stmt.close();
	        } catch (SQLException e ) {
	            System.out.println(e);
							invalid_input = 1;
	        }
				}

				if (invalid_input != 1) {
					for (int j = 0; j < player_ids.size(); j++) {
							query = "insert into results (event_id, player_id, medal, result) values ('"+event_id+"','"+player_ids.get(j)+"','"+medals.get(j)+"',"+results.get(j)+");";
							try {
									stmt = connection.createStatement();
									stmt.executeUpdate(query);
									stmt.close();
							} catch (SQLException e ) {
									System.out.println(e);
							}
					}

					System.out.println("Adding data from " + json + " into the database");
				}
			}
		}
		catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void main(String[] argv) {
		Scanner in_scanner = new Scanner(System.in);

		while(in_scanner.hasNext()) {
			String json = in_scanner.nextLine();
			processJSON(json);
		}
	}
}
