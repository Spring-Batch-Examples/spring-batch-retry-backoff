package com.rudra.aks.batch.config;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.rudra.aks.batch.model.UserBO;

@Configuration
@Import({DBConfig.class})
@EnableBatchProcessing
public class BatchConfig {

	@Autowired
	DataSource	dataSource;
    
    @Autowired
    private JobBuilderFactory jobs;
 
    @Autowired
    private StepBuilderFactory steps;
 

    /**
     * ItemReader to be used in chunk processing
     * 
     * @return	a FlatFileItemReader to read context from txt file and parse into UserBO
     * @throws  UnexpectedInputException
     * 			ParseException
     */
    @Bean
    public ItemReader<UserBO> itemReader() throws UnexpectedInputException, ParseException {

    	FlatFileItemReader<UserBO> reader = new FlatFileItemReader<UserBO>();
    	//reader.setLinesToSkip(1);
    	reader.setResource(new ClassPathResource("/record.txt"));
    	
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        String[] tokens = { "userid", "username", "emailid" };
        tokenizer.setNames(tokens);
        tokenizer.setDelimiter(",");
        
        BeanWrapperFieldSetMapper<UserBO> fieldSetMapper = new BeanWrapperFieldSetMapper<UserBO>();
        fieldSetMapper.setTargetType(UserBO.class);
        
        DefaultLineMapper<UserBO> lineMapper = new DefaultLineMapper<UserBO>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        reader.setLineMapper(lineMapper);
        reader.setSaveState(false);
        return reader;
    }
 
    /**
     * Simple ItemProcessor implementation
     * used for processing input dat before writing to db.
     * 
     * @return  same object, no processing
     */
    @Bean
    public ItemProcessor<UserBO, UserBO> itemProcessor() {
        return new ItemProcessor<UserBO, UserBO>() {
			public UserBO process(UserBO item) throws Exception {
				return item;
			}
        };
    }
 
    /**
     * Item writer implementation using 
     * JdbcBatchItemWriter to write txt data into db.
     * 
     * 
     * @return  an item writer
     */
    @Bean
    public ItemWriter<UserBO>	dbItemWriter() {
    	JdbcBatchItemWriter<UserBO> dbWriter = new JdbcBatchItemWriter<UserBO>();
    	dbWriter.setDataSource(dataSource);
    	dbWriter.setSql("insert into USER_BATCH(userid, username, emailid) values (:userid, :username, :emailid)");
    	dbWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<UserBO>());
    	return dbWriter;
    }
    
    /**
     * Step creating with defined reader, processor & writer
     * {@link #itemReader()}, {@link #itemProcessor()} & {@link #dbItemWriter()}
     * 
     * @param   reader
     * 			processor
     * 			writer
     * @return  a single step to be executed as first step of the Job.
     */
    @Bean
    protected Step step1(ItemReader<UserBO> reader, ItemProcessor<UserBO, UserBO> processor, ItemWriter<UserBO> writer) {
        return steps.get("step1")
        			.<UserBO, UserBO> chunk(5)
        			.reader(reader)
        			.processor(processor)
        			.writer(writer)
        			.faultTolerant()
        			/**
        			 * simple retry for the given exception while executing the chunk
        			 * retryLimit is number of times to retry for that item if got same exception.
        			 * 
        			 */
        			.retry(Exception.class)
        			.retryLimit(3)
        			/**
        			 * Giving RetryPolicy, it will override any retryLimit in this configuration
        			 */
        			.retryPolicy(simpleRetry())
        			/**
        			 * Providing a Backoff policy, see below implementation
        			 * To specify time interval between retry which is configured above.
        			 */
        			.backOffPolicy(backOffPolicy())
        			.skip(Exception.class)
        			.skipLimit(2)
        			/**
        			 * Enable mulit-thread exection of chunk for batch processing
        			 * @see taskExecutor() implementation .
        			 */
        			.taskExecutor(taskExecutor())
        			.build();
    }
    
    /**
     * Creating retry policy by using spring batch given retry policies
     * SimpleRetryPolicy constructed with no. of attempts & map of exceptions to be retry.
     * 
     * @return	a custom retry policy
     */
    @Bean("simpleRetry")
    public RetryPolicy simpleRetry() {
    	Map<Class<? extends Throwable>, Boolean> ex = new HashMap<Class<? extends Throwable>, Boolean>();
		ex.put(Exception.class, true);
		ex.put(BadSqlGrammarException.class, true);
		ex.put(FlatFileParseException.class, true);
		
    	SimpleRetryPolicy policy = new SimpleRetryPolicy(4, ex);
		policy.setMaxAttempts(4);
    	return policy;
	}

	/**
     * Setting BackOffPolicy with fixed delay & exponential delay
     * 
     * exponential delay will start with 1.5 secs till max 10 secs 
     * with interval of double the previous retry time.
     * 
     */
    @Bean("backOff")
    public BackOffPolicy backOffPolicy() {
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(12000);
		
		ExponentialBackOffPolicy expBackOffPolicy2 = new ExponentialBackOffPolicy();
		expBackOffPolicy2.setInitialInterval(1500);
		expBackOffPolicy2.setMultiplier(4);
		expBackOffPolicy2.setMaxInterval(10000);
		
    	return expBackOffPolicy2;
	}
    
    @Bean
    public	TaskExecutor	taskExecutor() {
    			
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(2);
		taskExecutor.setMaxPoolSize(3);
    	taskExecutor.setThreadNamePrefix("job-thread");
    	return taskExecutor;
    }
    
    /**
     * A Simple Job definition to executed by JobLauncher with 
     * step defined above. {@link #step1(ItemReader, ItemProcessor, ItemWriter)}
     * 
     * @param step1
     * @return
     * 
     * {@see #JobLauncher}
     */
    @Bean(name = "firstBatchJob")
    public Job job(@Qualifier("step1") Step step1) {
    	return jobs.get("firstBatchJob")
    			.incrementer(new RunIdIncrementer())
    			.start(step1)
    			.build();
    }
    
    
}
