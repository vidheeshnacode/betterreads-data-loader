package io.div.betterreadsdataloader;

import connection.DataStaxAstraProperties;
import io.div.betterreadsdataloader.author.Author;
import io.div.betterreadsdataloader.author.AuthorRepository;
import io.div.betterreadsdataloader.book.Book;
import io.div.betterreadsdataloader.book.BookRepository;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.work}")
	private String workDumpLocation;

	private void initAuthors(){
		Path path = Paths.get(authorDumpLocation);
		try(Stream<String> lines = Files.lines(path)){
			lines.forEach(line -> {
				// Read and Parse the line
				String jsonString = line.substring(line.indexOf("{"));

				try {
					JSONObject jsonObject = new JSONObject(jsonString);

					// Construct Author Object
					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));
					author.setId(jsonObject.optString("key").replace("/authors/", ""));

					// Persist using Repository
					System.out.println("Saving author " + author.getName()+ ".....");
					authorRepository.save(author);
				} catch (JSONException e){
					e.printStackTrace();
				}
			});
		}

		catch (IOException e){
			e.printStackTrace();
		}
	}

	private void initWorks(){
		Path path = Paths.get(workDumpLocation);

		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

		try(Stream<String> lines = Files.lines(path)){
			lines.limit(50).forEach(line -> {
				// Read and Parse the line
				String jsonString = line.substring(line.indexOf("{"));

				try {
					JSONObject jsonObject = new JSONObject(jsonString);

					// Construct Book Object
					Book book = new Book();

					book.setId(jsonObject.getString("key").replace("/works/", ""));

					book.setName(jsonObject.optString("title"));
					JSONObject descriptionObj = jsonObject.optJSONObject("description");
					if (descriptionObj != null){
						book.setDescription(descriptionObj.optString("value"));
					}

					JSONObject publishedObj = jsonObject.optJSONObject("created");
					if (publishedObj != null){
						String dateStr = publishedObj.optString("value");
						book.setPublishedDate(LocalDate.parse(dateStr, dateFormat));
					}

					JSONArray coversJSONArr = jsonObject.optJSONArray("covers");
					if (coversJSONArr != null){
						List<String> coverIds = new ArrayList<>();
						for(int i = 0; i < coversJSONArr.length(); i++){
							coverIds.add(coversJSONArr.optString(i));
						}
						book.setCoverIds(coverIds);
					}

					JSONArray authorsJSONArr = jsonObject.optJSONArray("authors");
					if (authorsJSONArr != null){
						List<String> authorIds = new ArrayList<>();
						for(int i = 0; i < authorsJSONArr.length(); i++){
							String authorId = authorsJSONArr.getJSONObject(i).getJSONObject("author").getString("key").replace("/authors/", "");
							authorIds.add(authorId);
						}
						book.setAuthorIds(authorIds);
						List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id)).
								map(optionalAuthor -> {
									if(!optionalAuthor.isPresent()) return "Author Unknowm";
									return optionalAuthor.get().getName();
								}).collect(Collectors.toList());

						book.setAuthorNames(authorNames);

						// Persist using Repository
						System.out.println("Saving book " + book.getName()+ ".....");
						bookRepository.save(book);
					}

				} catch (Exception e){
					e.printStackTrace();
				}
			});
		}

		catch (IOException e){
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}

	@PostConstruct
	public void start(){
		//initAuthors();
		initWorks();


		System.out.println(authorDumpLocation);

	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}



}
