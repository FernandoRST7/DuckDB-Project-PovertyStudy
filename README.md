# 1. Uma justificativa para a escolha do banco para o cenário atribuído à sua equipe. Essa **deve** conter uma discussão incluindo **todos** os seguintes aspectos: forma de armazenamento de arquivos, linguagem e processamento de consultas, processamento e controle de transações, mecanismos de recuperação e segurança.   

Nosso grupo pegou o cenário A do projeto 2. Neste cenário era pedido uma abordagem para realizar análises em grandes volumes de dados imutáveis, em que as consultas acessam um número pequenos de atributos que possuem grande volume de registros.  
Esses dados sendo imutáveis e históricos, por sua vez, são submetidos a baixa frequência de escrita ou atualização, porém grande frequência de leitura que devem ser confiáveis.

Diante dessas características, optamos pelo uso do **DuckDB**, um sistema de gerenciamento de banco de dados analítico **in-process**, leve, moderno e altamente eficiente para análises locais. A seguir, discutimos essa escolha sob os aspectos de **armazenamento**, **linguagem e processamento de consultas**, **controle de transações**, **mecanismos de recuperação** e **segurança**.

### 🔹 Forma de Armazenamento de Arquivos

O DuckDB utiliza um modelo **colunar** de armazenamento, o que o torna ideal para workloads analíticos onde apenas algumas colunas de muitos registros são lidas em cada consulta. Ele pode trabalhar com seu próprio formato binário local (arquivos `.duckdb`) e também **acessar diretamente arquivos no formato Parquet**, sem necessidade de importação. Esse formato colunar permite compressão eficiente e acelera operações de agregação e filtragem — exatamente o tipo de operação predominante no cenário proposto.

Além disso, por ser **in-process**, o DuckDB pode operar diretamente dentro de um script Python ou R, mantendo os dados em memória quando necessário, e evitando a sobrecarga de uma arquitetura cliente-servidor.

### 🔹 Linguagem e Processamento de Consultas

O DuckDB utiliza **SQL padrão ANSI**, oferecendo uma linguagem expressiva e poderosa para realizar consultas complexas, agregações, joins e filtros. O motor de execução é **vetorizado**, ou seja, processa blocos de dados em lotes (em vez de linha a linha), o que proporciona altíssimo desempenho, principalmente em operações analíticas típicas de Data Science e BI.

Além disso, há integração nativa com estruturas como **DataFrames do Pandas** e bibliotecas como `pyarrow`, `numpy` e `dplyr` no R. Isso facilita o uso por analistas de dados que já trabalham com notebooks Jupyter ou RStudio, permitindo uma curva de aprendizado mínima e grande produtividade.

### 🔹 Processamento e Controle de Transações

Embora o foco principal do DuckDB seja leitura e análise, ele implementa um modelo transacional baseado no conceito de **ACID**. Para garantir consistência e integridade em operações de escrita (ainda que raras neste cenário), o DuckDB aplica o modelo **MVCC (Multi-Version Concurrency Control)**, permitindo transações simultâneas com isolamento apropriado.

No contexto do cenário A, onde a escrita é rara e controlada, o modelo de transações do DuckDB é mais do que suficiente para garantir **consistência eventual** e confiabilidade nas leituras, sem a complexidade e sobrecarga de sistemas OLTP tradicionais.

### 🔹 Mecanismos de Recuperação

O DuckDB implementa **checkpointing e write-ahead logging (WAL)**, que são mecanismos fundamentais para garantir a durabilidade das operações. Mesmo que a frequência de escrita seja baixa, esses mecanismos asseguram que qualquer escrita realizada possa ser recuperada corretamente em caso de falhas.

Os checkpoints permitem que o banco persista o estado do banco de tempos em tempos, enquanto o WAL garante que todas as alterações sejam registradas antes de serem aplicadas ao banco — respeitando a propriedade de durabilidade do modelo ACID. Isso traz segurança mesmo para dados que são eventualmente atualizados.

### 🔹 Segurança

Como o DuckDB opera tipicamente em modo local, embutido dentro de aplicações ou notebooks, o modelo de segurança se foca mais em **controle de acesso ao arquivo** e boas práticas de isolamento no sistema operacional. Ele **não é projetado para múltiplos usuários concorrentes via rede**, como bancos de dados cliente-servidor (e isso está de acordo com os requisitos do cenário, onde isso não é exigido).

Entretanto, por operar em arquivos locais, ele pode se integrar facilmente a mecanismos externos de criptografia de disco, controle de permissões por sistema de arquivos, versionamento via Git ou backups em nuvem — estratégias que já fazem parte do workflow típico de cientistas de dados.

# 2. O modelo **lógico** para esse novo banco de dados, considerando sua forma de abstrair os dados (documentos, tabelas, e grafos).  



# 3. O modelo **físico** (script de criação do banco) e **populado** (com os mesmos dados do projeto anterior)  



# 4. Cinco **consultas** não triviais, que podem ou não ser as mesmas anteriores, desde que façam sentido para o novo cenário.