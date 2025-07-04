# �u���Ώۂ̃f�B���N�g���i�K�v�ɉ����ĕύX�j
$targetPath = "C:\Users\seiji_yfc8940\final\final\docs\claude"  # �� ������ύX

# �u���O�ƒu����̕�����
$oldText = "seijim"
$newText = "owner"  # �C�ӂ̒u��������ɕύX�\

# �Ώۃt�@�C���̊g���q�i��: txt, md, cs �Ȃǁj
$extensions = @("*.md", "*.txt", "*.cs", "*.json", "*.yaml", "*.yml","*.mhtml")

# �Ώۃt�@�C�����ċA�I�Ɍ������A�u�����������s
foreach ($ext in $extensions) {
    Get-ChildItem -Path $targetPath -Recurse -Filter $ext -File | ForEach-Object {
        $content = Get-Content $_.FullName -Raw
        if ($content -like "*$oldText*") {
            $content -replace [regex]::Escape($oldText), $newText | Set-Content $_.FullName
            Write-Host "? Replaced in: $($_.FullName)"
        }
    }
}
