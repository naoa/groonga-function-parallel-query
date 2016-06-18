# Groonga parallel_query function

複数のクエリをマルチスレッドで並列に検索し、検索結果を直列にマージする。

検索にあまり時間がかからず、2つめ以降のクエリの検索結果数が非常に多いようなケースではマージのコストの方が高くなり、遅くなることもある。

２つ目以降のクエリの検索結果が非常に多いと、マージのループが長くなるため、１つ目のクエリに検索結果が多くなるものをもってくるとよいかも。

デフォルトのワーカー数は8

環境変数
``GRN_PARALLEL_QUERY_N_WORKER``

検証中。

## Usage

```
parallel_query(
 "match_columns", "query",
 "match_columns", "query",
 ...,
 "AND|OR|NOT|ADJUST"
)
```

## Install

Install libgroonga-dev.

Build this function.

    % sh autogen.sh
    % ./configure
    % make
    % sudo make install

## Usage

Register `functions/parallel_query`:

    % groonga DB
    > register functions/parallel_query

## Author

Naoya Murakami naoya@createfield.com

## License

LGPL 2.1. See COPYING-LGPL-2.1 for details.
