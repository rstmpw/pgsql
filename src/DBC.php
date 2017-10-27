<?php
declare(strict_types=1);

namespace rstmpw\pgsql;


class DBC
{

    protected $connHandle = false;

    # DSN = pgsql:host=example.com;port=5432;dbname=testdb;user=bruce;password=mypass;connect_timeout=5
    # DSN = pgsql:host=/tmp;port=6432;dbname=testdb;user=bruce;password=mypass;connect_timeout=5
    public function __construct(string $DSNString)
    {
        $DSNString = str_replace(['pgsql:', ';'], ['', ' '], $DSNString);
        $this->connHandle = pg_connect($DSNString, PGSQL_CONNECT_FORCE_NEW);
        if (!$this->connHandle) throw new \RuntimeException('PGSQL: Connection failed for DSN:' . $DSNString);
    }

    public function __destruct()
    {
        if ($this->connHandle) pg_close($this->connHandle);
    }

    public function unescapeBytea(string &$data): void
    {
        $data = pg_unescape_bytea($data);
    }

    public function query(string $query, array &$params = null): DBResult
    {
        $startTime = microtime(true);
        if ($params) {
            $result = pg_query_params($this->connHandle, $query, $params);
        } else {
            $result = pg_query($this->connHandle, $query);
        }
        $queryTime = round(microtime(true) - $startTime, 3);

        if (!$result) {
            throw new \RuntimeException('PGSQL: Query error: ' . pg_last_error($this->connHandle));
        }

        return new DBResult($result, $queryTime);
    }

    public function insert(string $tblName, array &$row): DBResult
    {
        if (!$row) throw new \InvalidArgumentException('PGSQL: Size of $row must be greater than zero');

        $params = $fields = $values = [];
        $i = 0;

        foreach ($row as $fieldName => $value) {
            $params[$i] =& $row[$fieldName];
            $fields[] = $fieldName;
            $values[] = '$' . ++$i;
        }

        $sqlStmt = 'INSERT INTO ' . $tblName . ' ("' . implode('", "', $fields) . '") VALUES (' . implode(', ', $values) . ');';
        return $this->query($sqlStmt, $params);
    }

    public function select(string $tblName, array $fields = null, array $filterCond = null, array $orderBy = null, int $limit = null, int $offset = null): DBResult
    {
        if (!$fields) $fields = '*';
        else $fields = '"' . implode('", "', $fields) . '"';

        $preparedCond = $this->prepFilterCond($filterCond);
        $sqlStmt = 'SELECT ' . $fields . ' FROM ' . $tblName . ' WHERE ' . $preparedCond['sqlString'];

        if ($orderBy) {
            $orderSql = [];
            foreach ($orderBy as $field => $direction) {
                $direction = strtoupper($direction);
                if ($direction != 'ASC' && $direction != 'DESC')
                    throw new \InvalidArgumentException('PGSQL: $orderBy value must be ASC or DESC');
                $orderSql[] = $field . ' ' . $direction;
            }
            $sqlStmt .= ' ORDER BY ' . implode(', ', $orderSql);
        }

        if (!is_null($limit)) {
            if ($limit < 1) throw new \InvalidArgumentException('PGSQL: $limit must be greater than zero');
            $sqlStmt .= ' LIMIT ' . $limit;
        }

        if (!is_null($offset)) {
            if ($offset < 0) throw new \InvalidArgumentException('PGSQL: offset must be greater than or equal zero');
            $sqlStmt .= ' OFFSET ' . $offset;
        }

        return $this->query($sqlStmt . ';', $preparedCond['params']);
    }

    public function update(string $tblName, array $filterCond, array $updateFields): DBResult
    {
        $preparedCond = $this->prepFilterCond($filterCond);

        $params =& $preparedCond['params'];
        $i = count($params);
        $setItems = [];
        foreach ($updateFields as $field => $value) {
            $params[$i] = $value;
            $setItems[] = '"' . $field . '" = $' . ++$i;
        }
        $setItems = implode(', ', $setItems);
        $sqlStmt = 'UPDATE ' . $tblName . ' SET ' . $setItems . ' WHERE ' . $preparedCond['sqlString'];
        return $this->query($sqlStmt, $params);
    }

    public function delete(string $tblName, array $filterCond): DBResult
    {
        $preparedCond = $this->prepFilterCond($filterCond);

        $sqlStmt = 'DELETE FROM ' . $tblName . ' WHERE ' . $preparedCond['sqlString'];
        return $this->query($sqlStmt, $preparedCond['params']);
    }


    public function begin(): void
    {
        $this->query('BEGIN;');
    }

    public function commit(): void
    {
        $this->query('COMMIT;');
    }

    public function rollback(): void
    {
        $this->query('ROLLBACK;');
    }

    public function inTransaction(): bool
    {
        $stat = pg_transaction_status($this->connHandle);
        if ($stat == PGSQL_TRANSACTION_UNKNOWN) throw new \RuntimeException('PGSQL Connection broken');
        elseif ($stat == PGSQL_TRANSACTION_IDLE) return false;
        else return true;
    }

    protected function prepFilterCond(array &$filterCond): array
    {
        if (!$filterCond) return ['sqlString' => 'true', 'params' => []];

        $filterParts = [];
        $params = [];
        $i = 0;
        foreach ($filterCond as $field => $condition) {
            if (is_array($condition)) {
                $inParts = [];
                foreach ($condition as $value) {
                    $params[$i] = $value;
                    $inParts[] = '$' . ++$i;
                }
                $filterParts[] = '"' . $field . '" IN (' . implode(', ', $inParts) . ')';

            } else {
                $params[$i] = $condition;
                $filterParts[] = '"' . $field . '" = $' . ++$i;
            }
        }
        $filterString = implode(' AND ', $filterParts);
        return ['sqlString' => $filterString, 'params' => $params];
    }
}