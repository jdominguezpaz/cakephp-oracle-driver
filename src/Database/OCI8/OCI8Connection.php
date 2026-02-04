<?php
/**
 * Copyright 2015 - 2016, Cake Development Corporation (http://cakedc.com)
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * @copyright Copyright 2015 - 2016, Cake Development Corporation (http://cakedc.com)
 * @license MIT License (http://www.opensource.org/licenses/mit-license.php)
 */

namespace CakeDC\OracleDriver\Database\OCI8;

use Cake\Core\InstanceConfigTrait;
use PDO;

/**
 * OCI8 implementation of the Connection interface.
 */
class OCI8Connection extends PDO
{
    use InstanceConfigTrait;

    /**
     * Whether currently in a transaction
     *
     * @var bool
     */
    protected bool $_inTransaction = false;

    /**
     * Database connection.
     *
     * @var mixed OCI8 connection resource
     */
    protected mixed $dbh;

    /**
     * @var int
     */
    protected int $executeMode = OCI_COMMIT_ON_SUCCESS;

    protected array $_defaultConfig = [];

    /**
     * Creates a Connection to an Oracle Database using oci8 extension.
     *
     * @param string $dsn Oracle connection string in oci_connect format.
     * @param string $username Oracle username.
     * @param string $password Oracle user's password.
     * @param array $options Additional connection settings.
     *
     * @throws \CakeDC\OracleDriver\Database\OCI8\OCI8Exception
     */
    public function __construct(string $dsn, string $username, string $password, array $options)
    {
        $persistent = !empty($options['persistent']);
        $charset = !empty($options['charset']) ? $options['charset'] : null;
        $sessionMode = !empty($options['sessionMode']) ? (int)$options['sessionMode'] : OCI_DEFAULT;

        if ($persistent) {
            $this->dbh = @oci_pconnect($username, $password, $dsn, $charset, $sessionMode);
        } else {
            $this->dbh = @oci_connect($username, $password, $dsn, $charset, $sessionMode);
        }

        if (!$this->dbh) {
            throw OCI8Exception::fromErrorInfo(oci_error());
        }

        $this->setConfig($options);
    }

    /**
     * Returns database connection.
     *
     * @return mixed OCI8 connection resource
     */
    public function dbh(): mixed
    {
        return $this->dbh;
    }

    /**
     * Returns oracle version.
     *
     * @throws \UnexpectedValueException if the version string returned by the database server does not parsed
     * @return string Version number
     */
    public function getServerVersion(): string
    {
        $versionData = oci_server_version($this->dbh);
        if (!preg_match('/\s+(\d+\.\d+\.\d+\.\d+\.\d+)\s+/', $versionData, $version)) {
            throw new \UnexpectedValueException(__('Unexpected database version string "{0}" that not parsed.', $versionData));
        }

        return $version[1];
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $query, array $options = []): OCI8Statement|false
    {
        return new OCI8Statement($this->dbh, $query, $this);
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): OCI8Statement|false
    {
        $stmt = $this->prepare($query);
        $stmt->execute();

        return $stmt;
    }

    /**
     * {@inheritdoc}
     */
    public function quote(string $string, int $type = PDO::PARAM_STR): string|false
    {
        if (is_numeric($string)) {
            return $string;
        }
        $string = str_replace("'", "''", $string);

        return "'" . addcslashes($string, "\000\n\r\\\032") . "'";
    }

    /**
     * {@inheritdoc}
     */
    public function exec(string $statement): int|false
    {
        $stmt = $this->prepare($statement);
        $stmt->execute();

        return $stmt->rowCount();
    }

    /**
     * Returns the current execution mode.
     *
     * @return int
     */
    public function getExecuteMode(): int
    {
        return $this->executeMode;
    }

    /**
     * Returns true if the current process is in a transaction
     *
     * @deprecated Use inTransaction() instead
     * @return bool
     */
    public function isTransaction(): bool
    {
        return $this->inTransaction();
    }

    /**
     * {@inheritdoc}
     */
    public function inTransaction(): bool
    {
        return $this->executeMode == OCI_NO_AUTO_COMMIT;
    }

    /**
     * {@inheritdoc}
     */
    public function beginTransaction(): bool
    {
        $this->executeMode = OCI_NO_AUTO_COMMIT;

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function commit(): bool
    {
        if (!oci_commit($this->dbh)) {
            throw OCI8Exception::fromErrorInfo($this->errorInfo());
        }
        $this->executeMode = OCI_COMMIT_ON_SUCCESS;

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function rollBack(): bool
    {
        if (!oci_rollback($this->dbh)) {
            throw OCI8Exception::fromErrorInfo($this->errorInfo());
        }
        $this->executeMode = OCI_COMMIT_ON_SUCCESS;

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function errorCode(): string
    {
        $error = oci_error($this->dbh);
        if ($error !== false) {
            return (string)$error['code'];
        }

        return '00000';
    }

    /**
     * {@inheritdoc}
     */
    public function errorInfo(): array|false
    {
        return oci_error($this->dbh);
    }
}
