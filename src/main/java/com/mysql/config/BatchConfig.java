package com.mysql.config;

import com.mysql.models.Product;
import com.mysql.services.ProductRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;


import javax.persistence.EntityManagerFactory;

@Configuration
@ComponentScan({"com.mysql.services"})
@EnableJpaRepositories(basePackages = "com.mysql.services")
@EntityScan(basePackages = "com.mysql.models")
public class BatchConfig {

    @Autowired
    EntityManagerFactory emf;

    @Autowired
    ProductRepository productRepository;

    @Autowired
    public JobBuilderFactory jbf;

    @Autowired
    public StepBuilderFactory sbf;

    @Bean
    public FlatFileItemReader reader() {
        FlatFileItemReader reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("products.csv"));
        reader.setLinesToSkip(1);

        DefaultLineMapper lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("productName", "retailPrice");

        BeanWrapperFieldSetMapper fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Product.class);

        lineMapper.setFieldSetMapper(fieldSetMapper);
        lineMapper.setLineTokenizer(tokenizer);
        reader.setLineMapper(lineMapper);

        return reader;
    }

    @Bean
    public ItemProcessor<Product, Product> processor() {
        return (item) -> {
            item.setProductName(item.getProductName().toUpperCase());
            item.setRetailPrice(Double.toString(item.getRetailPrice() * 1.05));
            return item;
        };
    }

    @Bean
    public JpaItemWriter writer() {
        JpaItemWriter writer = new JpaItemWriter();
        writer.setEntityManagerFactory(emf);
        return writer;
    }


    @Bean
    public Job firstJob()
    {
        return jbf.get("job4")
                .incrementer(new RunIdIncrementer())
                .flow(firstStep())
                .end()
                .build();
    }

    @Bean
    public Step firstStep()
    {
        return sbf.get("step1")
                .<Product, Product>chunk(1)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }
}
