package me.prince.springbatch;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import com.opencsv.ResultSetHelperService;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchApplication {

    @Autowired
    DataSource dataSource;

    @Autowired
    StepBuilderFactory stepFactory;

    @Autowired
    JobBuilderFactory jobFactory;

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    JdbcTemplate jdbcTemplate;

    public static void main(String[] args) {
        SpringApplication.run(SpringBatchApplication.class, args);
    }

    @Bean
    CommandLineRunner commandLineRunner(JdbcTemplate jdbcTemplate) {
        return args -> {
            List<String> show_tables = jdbcTemplate.queryForList("show tables", String.class);
            show_tables.stream().filter(s -> !s.toLowerCase().startsWith("batch")).forEach(s -> {
                try {
                    processTable(s);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        };
    }

    private void processTable(String tableName) throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
        JdbcCursorItemReader<String> reader = new JdbcCursorItemReader();
        reader.setDataSource(dataSource);
        reader.setSql("select * from " + tableName);
        reader.setRowMapper(new MyCsvRowMapper());
        reader.setVerifyCursorPosition(false);

        Job jobTest = jobFactory.get("jobTest_" + tableName)
                .incrementer(new RunIdIncrementer())
                .listener(new JobExecutionListenerSupport())
                .flow(loadStaff(stepFactory, reader, tableName))
                .end()
                .build();
        jobLauncher.run(jobTest, new JobParameters());
    }

    private Step loadStaff(StepBuilderFactory stepFactory, JdbcCursorItemReader<String> reader, String tableName) {
        return stepFactory.get("stepTest")
                .<String, String>chunk(100)
                .reader(reader)
                .processor(new MyProcessor(tableName))
                .writer(new MyWriter())
                .build();
    }


}

class MyWriter implements org.springframework.batch.item.ItemWriter<String> {

    @Override
    public void write(List<? extends String> items) throws Exception {
//        items.forEach(s -> System.out.println(s));
    }
}


class MyProcessor implements ItemProcessor<String, String> {
    private String tableName;

    public MyProcessor(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String process(String item) throws Exception {
        System.out.println("---" + item);
        Path filePath = Path.of("files", tableName + ".csv");
        if (Files.exists(filePath)) {
            Files.writeString(filePath, item, StandardOpenOption.APPEND);
        } else {
            Files.writeString(filePath, item);
        }
        return null;
    }
}

class MyCsvRowMapper implements org.springframework.jdbc.core.RowMapper {
    @Override
    public String mapRow(ResultSet rs, int rowNum) throws SQLException {
        StringWriter writer = new StringWriter();
        CSVWriterBuilder builder = new CSVWriterBuilder(writer);
        ICSVWriter csvWriter = builder.build();
        try {
            var resultService = new ResultSetHelperService();
            if (rowNum == 1) {
                csvWriter.writeNext(resultService.getColumnNames(rs), true);
            }
            csvWriter.writeNext(resultService.getColumnValues(rs, true), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return writer.toString();
    }
}

@Configuration
class TTT {
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties memberDataSourceProperties() {
        return new DataSourceProperties();
    }
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource.configuration")
    public DataSource memberDataSource() {
        return memberDataSourceProperties().initializeDataSourceBuilder()
                .type(HikariDataSource.class).build();
    }

    @Bean
    @ConfigurationProperties("spring.second")
    public DataSourceProperties second() {
        return new DataSourceProperties();
    }
    @Bean
    @ConfigurationProperties("spring.second.configuration")
    public DataSource secondDataSource() {
        return second().initializeDataSourceBuilder()
                .type(HikariDataSource.class).build();
    }
}

@Configuration
class BatchConfiguration extends DefaultBatchConfigurer {
    @Autowired
    @Qualifier("secondDataSource")
    private DataSource dataSource;

    @Autowired
    @Override
    public void setDataSource(DataSource dataSource) {
        // override to do not set datasource even if a datasource exist.
        // initialize will use a Map based JobRepository (instead of database)
        super.setDataSource(this.dataSource);
    }

    /*@Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSource primaryDataSource() {
        return DataSourceBuilder.create().build();
    }*/
//
//    @Bean
//    @ConfigurationProperties(prefix = "spring.second")
//    public DataSource secondaryDataSource() {
//        return DataSourceBuilder.create().build();
//    }
}


