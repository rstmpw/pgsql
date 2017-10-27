<?php
declare(strict_types=1);

namespace rstmpw\pgsql;


class DBResult
{
    protected $result = false;
    protected $queryTime = null;
    protected $affectedRows = null;
    protected $numRows = null;

    public function __construct(resource $result, float $queryTime = null)
    {
        $this->result = $result;
        $this->queryTime = $queryTime;
    }

    public function __destruct()
    {
        pg_free_result($this->result);
        $this->result = false;
    }

    public function __get($name)
    {
        switch ($name) {
            case 'numRows':
                if (is_null($this->numRows)) {
                    $this->numRows = pg_num_rows($this->result);
                }
                return $this->numRows;
                break;
            case 'affectedRows':
                if (is_null($this->affectedRows)) {
                    $this->affectedRows = pg_affected_rows($this->result);
                }
                return $this->affectedRows;
                break;
            case 'queryTime':
                return $this->queryTime;
                break;
            default:
                throw new \LogicException('Undefined property');
        }
    }

    public function fetchRow(): array
    {
        return pg_fetch_array($this->result, null, PGSQL_ASSOC);
    }

    public function fetchAll(): array
    {
        return pg_fetch_all($this->result);
    }
}