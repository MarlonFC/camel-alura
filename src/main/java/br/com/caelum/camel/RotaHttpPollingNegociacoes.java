package br.com.caelum.camel;

import java.text.SimpleDateFormat;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.xstream.XStreamDataFormat;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;
import com.thoughtworks.xstream.XStream;

public class RotaHttpPollingNegociacoes {

	public static void main(String[] args) throws Exception {
		
		SimpleRegistry registro = new SimpleRegistry();
		registro.put("mysql", criaDataSource());
		
		CamelContext context = new DefaultCamelContext(registro);//construtor recebe registro

		final XStream xstream = new XStream();
		xstream.alias("negociacao", Negociacao.class);

		context.addRoutes(new RouteBuilder() {

			@Override
			public void configure() throws Exception {
				from("timer://negociacoes?fixedRate=true&delay=1s&period=360s").
				//Trabalhando com file
				/*
				 * // split().xpath("/list/negociacao"). // filter(). //
				 * xpath("/list/negociacao"). // convertBodyTo(getClass()).unmarshal() //
				 * log("${body}"). // setHeader(Exchange.FILE_NAME,
				 * constant("negociacoes.xml")).
				 */				
				to("http4://argentumws-spring.herokuapp.com/negociacoes").
				convertBodyTo(String.class).
				unmarshal(new XStreamDataFormat(xstream)).
				split(body()).
				process(new Processor() {
			        @Override
			        public void process(Exchange exchange) throws Exception {
			            Negociacao negociacao = exchange.getIn().getBody(Negociacao.class);
			            exchange.setProperty("preco", negociacao.getPreco());
			            exchange.setProperty("quantidade", negociacao.getQuantidade());
								
						  String data = new
						  SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(negociacao.getData().getTime()
						  ); exchange.setProperty("data", data);				 
			        }
			      }).
			    setBody(simple("insert into negociacao(preco, quantidade, data) values (${property.preco}, ${property.quantidade}, '${property.data}')")).
			    log("${body}"). //logando o comando esql
			    delay(1000). //esperando 1s para deixar a execução mais fácil de entender
			    to("jdbc:mysql"); //usando o componente jdbc que envia o SQL para mysql
				
				//Trabalhando com file
				//setHeader(Exchange.FILE_NAME, constant("negociacoes.xml")).to("file:negociacoes");

			}
		});
		 context.start(); 
		Thread.sleep(20000);
		context.stop();

	}
		
	private static MysqlConnectionPoolDataSource criaDataSource() {
				
	    MysqlConnectionPoolDataSource mysqlDs = new MysqlConnectionPoolDataSource();
	    //mysqlDs.setDatabaseName("camel");
	    //mysqlDs.setServerName("localhost");
	    //Foi necessário alterar para URL com a inteção de add o timeZone e corrigir o erro -> The server time zone value 'Hora oficial do Brasil' is unrecognized or represents more than one time zone.
	    mysqlDs.setURL("jdbc:mysql://localhost/camel?useTimezone=true&serverTimezone=UTC");
	    mysqlDs.setPort(3306);
	    mysqlDs.setUser("root");
	    mysqlDs.setPassword("root");
	    return mysqlDs;
	}

}
